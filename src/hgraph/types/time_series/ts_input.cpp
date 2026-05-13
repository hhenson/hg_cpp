#include <hgraph/types/time_series/ts_input.h>

#include <cstdint>
#include <map>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>

namespace hgraph
{
    namespace
    {
        void validate_input_endpoint_schema(const TSEndpointSchema &endpoint_schema, bool root)
        {
            const auto *schema = endpoint_schema.schema();
            if (schema == nullptr) { throw std::invalid_argument("TSInput endpoint annotation requires a schema"); }

            if (root)
            {
                if (schema->kind != TSTypeKind::TSB || !endpoint_schema.is_non_peered())
                {
                    throw std::invalid_argument("TSInput root endpoint annotation must be a non-peered TSB");
                }
            }

            switch (endpoint_schema.role())
            {
                case TSEndpointRole::Peered:
                    return;

                case TSEndpointRole::NonPeered:
                    if (schema->kind != TSTypeKind::TSB && schema->kind != TSTypeKind::TSL)
                    {
                        throw std::invalid_argument(
                            "TSInput non-peered prefixes require TSB or fixed-size TSL schemas");
                    }
                    if (schema->kind == TSTypeKind::TSL && schema->fixed_size() == 0)
                    {
                        throw std::invalid_argument("TSInput non-peered TSL prefixes currently require a fixed size");
                    }
                    for (const auto &child : endpoint_schema.children())
                    {
                        validate_input_endpoint_schema(child, false);
                    }
                    return;
            }
        }

        void append_endpoint_key(std::string &key, const TSEndpointSchema &endpoint_schema)
        {
            const auto role = static_cast<std::uint8_t>(endpoint_schema.role());
            key.append(reinterpret_cast<const char *>(&role), sizeof(role));

            const auto schema_bits = reinterpret_cast<std::uintptr_t>(endpoint_schema.schema());
            key.append(reinterpret_cast<const char *>(&schema_bits), sizeof(schema_bits));

            if (endpoint_schema.is_non_peered())
            {
                const auto &children = endpoint_schema.children();
                const auto  size     = children.size();
                key.append(reinterpret_cast<const char *>(&size), sizeof(size));
                for (const auto &child : children) { append_endpoint_key(key, child); }
            }
        }

        [[nodiscard]] std::string plan_cache_key(const TSInputConstructionPlan &plan)
        {
            std::string key;
            key.reserve(128);
            append_endpoint_key(key, plan.endpoint_schema());
            return key;
        }
    }  // namespace

    namespace detail
    {
        struct TSInputActiveTarget;

        struct TSInputSchedulingNotifier final : Notifiable
        {
            TSInputActiveTarget *owner{nullptr};
            Notifiable          *target{nullptr};

            void notify(engine_time_t modified_time) override
            {
                if (target != nullptr) { target->notify(modified_time); }
            }

            void notify_invalidated() noexcept override;
        };

        struct TSInputActiveTarget
        {
            explicit TSInputActiveTarget(std::vector<std::size_t> path_)
                : path(std::move(path_))
            {
                notifier.owner = this;
            }

            TSInputActiveTarget(const TSInputActiveTarget &) = delete;
            TSInputActiveTarget &operator=(const TSInputActiveTarget &) = delete;

            ~TSInputActiveTarget() noexcept
            {
                unsubscribe();
            }

            void subscribe(TSDataView observed_, Notifiable *target)
            {
                if (observed_.valid() && observed_.data() == observed.data() && observed_.binding() == observed.binding())
                {
                    notifier.target = target;
                    return;
                }

                unsubscribe();
                notifier.target = target;
                if (target == nullptr || !observed_.valid()) { return; }
                observed = observed_;
                observed.subscribe(&notifier);
            }

            void unsubscribe() noexcept
            {
                if (!observed.valid()) { return; }
                try { observed.unsubscribe(&notifier); }
                catch (...) {}
                observed = {};
            }

            bool active{true};
            std::vector<std::size_t> path{};
            TSDataView observed{};
            TSInputSchedulingNotifier notifier{};
        };

        void TSInputSchedulingNotifier::notify_invalidated() noexcept
        {
            if (owner != nullptr) { owner->observed = {}; }
        }

        struct TSInputNode
        {
            struct TargetObserver final : Notifiable
            {
                explicit TargetObserver(TSInputNode *owner_) noexcept
                    : owner(owner_)
                {
                }

                void notify(engine_time_t modified_time) override
                {
                    if (owner != nullptr) { owner->record_modified(modified_time); }
                }

                void notify_invalidated() noexcept override
                {
                    if (owner != nullptr) { owner->target_invalidated(); }
                }

                TSInputNode *owner{nullptr};
            };

            TSInputNode(TSEndpointRole              role_,
                        const TSValueTypeMetaData  *schema_,
                        TSInputNode                *parent_,
                        std::size_t                 child_id_)
                : role(role_),
                  schema(schema_),
                  parent(parent_),
                  child_id(child_id_),
                  target_observer(this)
            {
            }

            TSInputNode(const TSInputNode &) = delete;
            TSInputNode &operator=(const TSInputNode &) = delete;

            ~TSInputNode() noexcept
            {
                unbind_target_noexcept();
            }

            [[nodiscard]] std::unique_ptr<TSInputNode> deep_copy(TSInputNode *new_parent) const
            {
                auto copy = std::make_unique<TSInputNode>(role, schema, new_parent, child_id);
                copy->last_modified_time = last_modified_time;
                copy->children.reserve(children.size());
                for (const auto &child : children)
                {
                    copy->children.push_back(child ? child->deep_copy(copy.get()) : nullptr);
                }
                return copy;
            }

            void relink(TSInputNode *new_parent) noexcept
            {
                parent = new_parent;
                target_observer.owner = this;
                for (auto &child : children)
                {
                    if (child) { child->relink(this); }
                }
                for (auto &[path, active] : active_targets)
                {
                    if (active) { active->notifier.owner = active.get(); }
                }
            }

            bool record_modified(engine_time_t modified_time)
            {
                if (modified_time == MIN_DT) { throw std::invalid_argument("TSInput modification requires a concrete time"); }
                if (last_modified_time == modified_time) { return false; }
                last_modified_time = modified_time;
                observers.notify(modified_time);
                if (parent != nullptr) { parent->record_modified(modified_time); }
                return true;
            }

            void bind_target(TSOutputView output)
            {
                if (role != TSEndpointRole::Peered)
                {
                    throw std::logic_error("TSInput target binding requires a peered terminal");
                }
                if (!output.bound())
                {
                    throw std::invalid_argument("TSInput target binding requires a bound output view");
                }
                if (!time_series_schema_equivalent(output.schema(), schema))
                {
                    throw std::invalid_argument("TSInput target binding schema does not match the input slot schema");
                }

                unbind_target();
                target = output;
                target.subscribe(&target_observer);
                if (target.last_modified_time() != MIN_DT) { last_modified_time = target.last_modified_time(); }
                resubscribe_active_targets();
            }

            void unbind_target()
            {
                unsubscribe_active_targets();
                if (target.bound())
                {
                    target.unsubscribe(&target_observer);
                }
                target = {};
            }

            void unbind_target_noexcept() noexcept
            {
                try { unbind_target(); }
                catch (...) {}
            }

            void target_invalidated() noexcept
            {
                target = {};
                for (auto &[path, active] : active_targets)
                {
                    if (active) { active->observed = {}; }
                }
            }

            void make_local_active(Notifiable *target_notifier)
            {
                if (locally_active)
                {
                    if (local_notifier.target == nullptr && target_notifier != nullptr) { observers.subscribe(&local_notifier); }
                    if (local_notifier.target != nullptr && target_notifier == nullptr) { observers.unsubscribe(&local_notifier); }
                    local_notifier.target = target_notifier;
                    return;
                }
                locally_active = true;
                local_notifier.owner = nullptr;
                local_notifier.target = target_notifier;
                if (target_notifier != nullptr) { observers.subscribe(&local_notifier); }
            }

            void make_local_passive()
            {
                if (!locally_active) { return; }
                if (local_notifier.target != nullptr) { observers.unsubscribe(&local_notifier); }
                local_notifier.target = nullptr;
                locally_active = false;
            }

            [[nodiscard]] bool local_active() const noexcept
            {
                return locally_active;
            }

            void make_target_active(const std::vector<std::size_t> &path, TSDataView observed, Notifiable *target_notifier)
            {
                auto &active = active_targets[path];
                if (!active) { active = std::make_unique<TSInputActiveTarget>(path); }
                active->active = true;
                active->subscribe(observed, target_notifier);
            }

            void make_target_passive(const std::vector<std::size_t> &path)
            {
                const auto it = active_targets.find(path);
                if (it == active_targets.end()) { return; }
                it->second->unsubscribe();
                active_targets.erase(it);
            }

            [[nodiscard]] bool target_active(const std::vector<std::size_t> &path) const noexcept
            {
                return active_targets.find(path) != active_targets.end();
            }

            [[nodiscard]] TSDataView target_child_at_path(const std::vector<std::size_t> &path) const
            {
                if (!target.bound()) { return {}; }
                TSDataView current = target.data_view();
                for (const auto index : path)
                {
                    const auto *schema = current.schema();
                    if (schema == nullptr) { return {}; }
                    if (schema->kind == TSTypeKind::TSB)
                    {
                        auto bundle = current.as_bundle();
                        current = bundle.at(index);
                    }
                    else if (schema->kind == TSTypeKind::TSL)
                    {
                        auto list = current.as_list();
                        current = list.at(index);
                    }
                    else
                    {
                        return {};
                    }
                }
                return current;
            }

            void resubscribe_active_targets()
            {
                for (auto &[path, active] : active_targets)
                {
                    if (!active) { continue; }
                    active->subscribe(target_child_at_path(path), active->notifier.target);
                }
            }

            void unsubscribe_active_targets() noexcept
            {
                for (auto &[path, active] : active_targets)
                {
                    if (active) { active->unsubscribe(); }
                }
            }

            TSEndpointRole role{TSEndpointRole::Peered};
            const TSValueTypeMetaData *schema{nullptr};
            TSInputNode *parent{nullptr};
            std::size_t child_id{TS_DATA_NO_CHILD_ID};
            std::vector<std::unique_ptr<TSInputNode>> children{};

            engine_time_t last_modified_time{MIN_DT};
            TSDataObserverSet observers{};

            TSOutputView target{};
            TargetObserver target_observer;

            bool locally_active{false};
            TSInputSchedulingNotifier local_notifier{};
            std::map<std::vector<std::size_t>, std::unique_ptr<TSInputActiveTarget>> active_targets{};
        };

        [[nodiscard]] std::unique_ptr<TSInputNode> make_node_from_endpoint_schema(
            const TSEndpointSchema &endpoint_schema,
            TSInputNode            *parent,
            std::size_t             child_id)
        {
            if (endpoint_schema.empty()) { return nullptr; }

            auto node = std::make_unique<TSInputNode>(
                endpoint_schema.role(),
                endpoint_schema.schema(),
                parent,
                child_id);
            if (endpoint_schema.is_non_peered())
            {
                node->children.reserve(endpoint_schema.children().size());
                for (std::size_t index = 0; index < endpoint_schema.children().size(); ++index)
                {
                    node->children.push_back(
                        make_node_from_endpoint_schema(endpoint_schema.children()[index], node.get(), index));
                }
            }
            return node;
        }
    }  // namespace detail

    TSInputConstructionPlan::TSInputConstructionPlan(const TSValueTypeMetaData &root_schema,
                                                     TSEndpointSchema           endpoint_schema)
        : schema_(&root_schema),
          endpoint_schema_(std::move(endpoint_schema))
    {
        if (!time_series_schema_equivalent(&root_schema, endpoint_schema_.schema()))
        {
            throw std::invalid_argument("TSInput construction annotation schema does not match the root schema");
        }
        validate_input_endpoint_schema(endpoint_schema_, true);
    }

    const TSValueTypeMetaData &TSInputConstructionPlan::schema() const noexcept
    {
        return *schema_;
    }

    const TSEndpointSchema &TSInputConstructionPlan::endpoint_schema() const noexcept
    {
        return endpoint_schema_;
    }

    TSInputConstructionPlan TSInputPlanFactory::compile(const TSValueTypeMetaData &root_schema,
                                                        const TSEndpointSchema    &endpoint_schema)
    {
        return TSInputConstructionPlan{root_schema, endpoint_schema};
    }

    TSInputBuilder::TSInputBuilder(TSInputConstructionPlan plan)
        : plan_(std::move(plan))
    {
    }

    const TSValueTypeMetaData &TSInputBuilder::schema() const noexcept
    {
        return plan_.schema();
    }

    TSInput TSInputBuilder::make_input() const
    {
        return TSInput{*this};
    }

    const TSInputBuilder *TSInputBuilderFactory::builder_for(const TSValueTypeMetaData &root_schema,
                                                            const TSEndpointSchema    &endpoint_schema)
    {
        return builder_for(TSInputPlanFactory::compile(root_schema, endpoint_schema));
    }

    const TSInputBuilder &TSInputBuilderFactory::checked_builder_for(const TSValueTypeMetaData &root_schema,
                                                                    const TSEndpointSchema    &endpoint_schema)
    {
        return checked_builder_for(TSInputPlanFactory::compile(root_schema, endpoint_schema));
    }

    const TSInputBuilder *TSInputBuilderFactory::builder_for(const TSInputConstructionPlan &plan)
    {
        if (plan.endpoint_schema().schema() == nullptr || plan.endpoint_schema().schema()->kind != TSTypeKind::TSB ||
            !plan.endpoint_schema().is_non_peered())
        {
            return nullptr;
        }

        static std::unordered_map<std::string, std::unique_ptr<TSInputBuilder>> cache;
        static std::mutex mutex;

        const auto key = plan_cache_key(plan);
        std::lock_guard lock{mutex};
        if (const auto it = cache.find(key); it != cache.end()) { return it->second.get(); }

        auto builder = std::unique_ptr<TSInputBuilder>(new TSInputBuilder(plan));
        const auto *result = builder.get();
        cache.emplace(key, std::move(builder));
        return result;
    }

    const TSInputBuilder &TSInputBuilderFactory::checked_builder_for(const TSInputConstructionPlan &plan)
    {
        if (const auto *builder = builder_for(plan); builder != nullptr) { return *builder; }
        throw std::invalid_argument("TSInputBuilderFactory requires a non-peered TSB root annotation");
    }

    TSInput::TSInput() noexcept = default;

    TSInput::TSInput(const TSInputBuilder &builder)
        : builder_(&builder)
    {
        rebuild_from_plan(builder.plan_);
    }

    TSInput::TSInput(const TSInputConstructionPlan &plan)
    {
        rebuild_from_plan(plan);
    }

    TSInput::TSInput(const TSInput &other)
        : builder_(other.builder_),
          schema_(other.schema_),
          root_(other.root_ ? other.root_->deep_copy(nullptr) : nullptr)
    {
    }

    TSInput &TSInput::operator=(const TSInput &other)
    {
        if (this == &other) { return *this; }
        TSInput replacement{other};
        return *this = std::move(replacement);
    }

    TSInput::TSInput(TSInput &&other) noexcept
        : builder_(std::exchange(other.builder_, nullptr)),
          schema_(std::exchange(other.schema_, nullptr)),
          root_(std::move(other.root_))
    {
        relink_nodes();
    }

    TSInput &TSInput::operator=(TSInput &&other) noexcept
    {
        if (this != &other)
        {
            root_.reset();
            builder_ = std::exchange(other.builder_, nullptr);
            schema_ = std::exchange(other.schema_, nullptr);
            root_ = std::move(other.root_);
            relink_nodes();
        }
        return *this;
    }

    TSInput::~TSInput() = default;

    bool TSInput::has_value() const noexcept
    {
        return root_ != nullptr;
    }

    const TSValueTypeMetaData *TSInput::schema() const noexcept
    {
        return schema_;
    }

    TSInputView TSInput::view(Notifiable *scheduling_notifier, engine_time_t evaluation_time)
    {
        return TSInputView{this, root_.get(), {}, {}, scheduling_notifier, evaluation_time};
    }

    TSInputView TSInput::view(Notifiable *scheduling_notifier, engine_time_t evaluation_time) const
    {
        return TSInputView{const_cast<TSInput *>(this), root_.get(), {}, {}, scheduling_notifier, evaluation_time};
    }

    void TSInput::rebuild_from_plan(const TSInputConstructionPlan &plan)
    {
        schema_ = &plan.schema();
        root_ = detail::make_node_from_endpoint_schema(plan.endpoint_schema(), nullptr, TS_DATA_NO_CHILD_ID);
        relink_nodes();
    }

    void TSInput::relink_nodes() noexcept
    {
        if (root_) { root_->relink(nullptr); }
    }

    TSInputView::TSInputView() noexcept = default;

    TSInputView::TSInputView(TSInput                  *input,
                             detail::TSInputNode     *node,
                             TSDataView               target_view,
                             std::vector<std::size_t> target_path,
                             Notifiable              *scheduling_notifier,
                             engine_time_t            evaluation_time) noexcept
        : input_(input),
          node_(node),
          target_view_(target_view),
          target_path_(std::move(target_path)),
          scheduling_notifier_(scheduling_notifier),
          evaluation_time_(evaluation_time)
    {
    }

    const TSValueTypeMetaData *TSInputView::schema() const noexcept
    {
        if (target_view_live()) { return target_view_.schema(); }
        return node_ != nullptr ? node_->schema : nullptr;
    }

    bool TSInputView::bound() const noexcept
    {
        if (node_ == nullptr) { return false; }
        if (is_target_position()) { return true; }
        if (node_->role == TSEndpointRole::Peered) { return node_->target.bound(); }
        return true;
    }

    bool TSInputView::valid() const
    {
        if (node_ == nullptr) { return false; }
        if (is_target_position()) { return target_view_.has_current_value(); }
        if (node_->role == TSEndpointRole::Peered)
        {
            return node_->target.bound() && node_->target.valid();
        }
        return all_valid();
    }

    bool TSInputView::all_valid() const
    {
        if (node_ == nullptr) { return false; }
        if (is_target_position()) { return target_view_.all_valid(); }
        if (node_->role == TSEndpointRole::Peered)
        {
            return node_->target.bound() && node_->target.all_valid();
        }
        if (node_->role != TSEndpointRole::NonPeered) { return false; }
        for (const auto &child : node_->children)
        {
            if (!child) { return false; }
            if (!TSInputView{input_, child.get(), {}, {}, scheduling_notifier_, evaluation_time_}.all_valid())
            {
                return false;
            }
        }
        return true;
    }

    engine_time_t TSInputView::last_modified_time() const
    {
        if (target_view_live()) { return target_view_.last_modified_time(); }
        if (node_ == nullptr) { return MIN_DT; }
        if (node_->role == TSEndpointRole::Peered && node_->target.bound())
        {
            return node_->target.last_modified_time();
        }
        return node_->last_modified_time;
    }

    bool TSInputView::modified(engine_time_t evaluation_time) const
    {
        if (evaluation_time == MIN_DT) { return false; }
        if (target_view_live()) { return target_view_.modified(evaluation_time); }
        if (node_ == nullptr) { return false; }
        return node_->last_modified_time == evaluation_time ||
               (node_->role == TSEndpointRole::Peered && node_->target.bound() &&
                node_->target.modified(evaluation_time));
    }

    bool TSInputView::modified() const
    {
        return modified(evaluation_time_);
    }

    ValueView TSInputView::value() const
    {
        if (target_view_live()) { return target_view_.value(); }
        if (node_ != nullptr && node_->role == TSEndpointRole::Peered && node_->target.bound())
        {
            return node_->target.value();
        }
        throw std::logic_error("TSInputView::value requires a bound peered terminal");
    }

    ValueView TSInputView::delta_value(engine_time_t evaluation_time) const
    {
        if (target_view_live()) { return target_view_.delta_value(evaluation_time); }
        if (node_ != nullptr && node_->role == TSEndpointRole::Peered && node_->target.bound())
        {
            return node_->target.delta_value(evaluation_time);
        }
        throw std::logic_error("TSInputView::delta_value requires a bound peered terminal");
    }

    ValueView TSInputView::delta_value() const
    {
        return delta_value(evaluation_time_);
    }

    void TSInputView::bind_output(const TSOutputView &output)
    {
        if (node_ == nullptr) { throw std::logic_error("TSInputView::bind_output requires a live input view"); }
        if (!is_target_root())
        {
            throw std::logic_error("TSInputView::bind_output must be called on a peered terminal root view");
        }
        node_->bind_target(output);
        target_view_ = node_->target.data_view();
    }

    void TSInputView::unbind_output()
    {
        if (node_ == nullptr) { return; }
        if (!is_target_root())
        {
            throw std::logic_error("TSInputView::unbind_output must be called on a peered terminal root view");
        }
        node_->unbind_target();
        target_view_ = {};
    }

    void TSInputView::make_active()
    {
        if (node_ == nullptr) { return; }
        if (!target_path_.empty())
        {
            node_->make_target_active(target_path_, target_view_live() ? target_view_ : TSDataView{}, scheduling_notifier_);
            if (scheduling_notifier_ != nullptr && evaluation_time_ != MIN_DT && modified(evaluation_time_))
            {
                scheduling_notifier_->notify(evaluation_time_);
            }
            return;
        }
        node_->make_local_active(scheduling_notifier_);
        if (scheduling_notifier_ != nullptr && evaluation_time_ != MIN_DT && modified(evaluation_time_))
        {
            scheduling_notifier_->notify(evaluation_time_);
        }
    }

    void TSInputView::make_passive()
    {
        if (node_ == nullptr) { return; }
        if (!target_path_.empty())
        {
            node_->make_target_passive(target_path_);
            return;
        }
        node_->make_local_passive();
    }

    bool TSInputView::active() const
    {
        if (node_ == nullptr) { return false; }
        if (!target_path_.empty()) { return node_->target_active(target_path_); }
        return node_->local_active();
    }

    TSBInputView TSInputView::as_bundle() &
    {
        return TSBInputView{*this};
    }

    TSBInputView TSInputView::as_bundle() const &
    {
        return TSBInputView{*this};
    }

    TSLInputView TSInputView::as_list() &
    {
        return TSLInputView{*this};
    }

    TSLInputView TSInputView::as_list() const &
    {
        return TSLInputView{*this};
    }

    bool TSInputView::is_target_position() const noexcept
    {
        return target_view_live();
    }

    bool TSInputView::target_view_live() const noexcept
    {
        return node_ != nullptr && node_->role == TSEndpointRole::Peered && node_->target.bound() &&
               target_view_.valid();
    }

    bool TSInputView::is_target_root() const noexcept
    {
        return node_ != nullptr && node_->role == TSEndpointRole::Peered && target_path_.empty();
    }

    TSInputView TSInputView::child_from_target(TSDataView child, std::size_t index) const
    {
        auto path = target_path_;
        path.push_back(index);
        return TSInputView{input_, node_, child, std::move(path), scheduling_notifier_, evaluation_time_};
    }

    TSInputView TSInputView::child_from_node(detail::TSInputNode *child) const noexcept
    {
        TSDataView target_view{};
        if (child != nullptr && child->role == TSEndpointRole::Peered && child->target.bound())
        {
            target_view = child->target.data_view();
        }
        return TSInputView{input_, child, target_view, {}, scheduling_notifier_, evaluation_time_};
    }

    TSBInputView::TSBInputView(TSInputView view)
        : view_(std::move(view))
    {
        const auto *meta = schema();
        if (meta == nullptr || meta->kind != TSTypeKind::TSB)
        {
            throw std::invalid_argument("TSBInputView requires a TSB input view");
        }
    }

    const TSInputView &TSBInputView::base() const noexcept
    {
        return view_;
    }

    TSInputView &TSBInputView::base() noexcept
    {
        return view_;
    }

    const TSValueTypeMetaData *TSBInputView::schema() const noexcept
    {
        return view_.schema();
    }

    std::size_t TSBInputView::size() const
    {
        const auto *meta = schema();
        return meta != nullptr ? meta->field_count() : 0;
    }

    bool TSBInputView::empty() const
    {
        return size() == 0;
    }

    bool TSBInputView::has_field(std::string_view name) const noexcept
    {
        return find_field_index(name) != npos;
    }

    TSInputView TSBInputView::at(std::size_t index) &
    {
        if (index >= size()) { throw std::out_of_range("TSBInputView::at index out of range"); }
        if (view_.is_target_position())
        {
            auto bundle = view_.target_view_.as_bundle();
            return view_.child_from_target(bundle.at(index), index);
        }
        if (view_.node_ == nullptr || view_.node_->role != TSEndpointRole::NonPeered)
        {
            throw std::logic_error("TSBInputView::at requires a non-peered bundle or bound target");
        }
        auto *child = index < view_.node_->children.size() ? view_.node_->children[index].get() : nullptr;
        if (child == nullptr) { throw std::logic_error("TSBInputView::at selected an unplanned input slot"); }
        return view_.child_from_node(child);
    }

    TSInputView TSBInputView::at(std::size_t index) const &
    {
        return const_cast<TSBInputView *>(this)->at(index);
    }

    TSInputView TSBInputView::operator[](std::size_t index) &
    {
        return at(index);
    }

    TSInputView TSBInputView::operator[](std::size_t index) const &
    {
        return at(index);
    }

    TSInputView TSBInputView::at(std::string_view name) &
    {
        return at(field_index(name));
    }

    TSInputView TSBInputView::at(std::string_view name) const &
    {
        return const_cast<TSBInputView *>(this)->at(name);
    }

    TSInputView TSBInputView::field(std::string_view name) &
    {
        return at(name);
    }

    TSInputView TSBInputView::field(std::string_view name) const &
    {
        return at(name);
    }

    TSInputView TSBInputView::operator[](std::string_view name) &
    {
        return at(name);
    }

    TSInputView TSBInputView::operator[](std::string_view name) const &
    {
        return at(name);
    }

    std::size_t TSBInputView::field_index(std::string_view name) const
    {
        const auto index = find_field_index(name);
        if (index == npos) { throw std::out_of_range("TSBInputView field not found"); }
        return index;
    }

    std::size_t TSBInputView::find_field_index(std::string_view name) const noexcept
    {
        const auto *meta = schema();
        if (meta == nullptr || meta->kind != TSTypeKind::TSB) { return npos; }
        for (std::size_t index = 0; index < meta->field_count(); ++index)
        {
            const auto *field_name = meta->fields()[index].name;
            if (field_name != nullptr && name == field_name) { return index; }
        }
        return npos;
    }

    TSLInputView::TSLInputView(TSInputView view)
        : view_(std::move(view))
    {
        const auto *meta = schema();
        if (meta == nullptr || meta->kind != TSTypeKind::TSL)
        {
            throw std::invalid_argument("TSLInputView requires a TSL input view");
        }
    }

    const TSInputView &TSLInputView::base() const noexcept
    {
        return view_;
    }

    TSInputView &TSLInputView::base() noexcept
    {
        return view_;
    }

    const TSValueTypeMetaData *TSLInputView::schema() const noexcept
    {
        return view_.schema();
    }

    std::size_t TSLInputView::size() const
    {
        const auto *meta = schema();
        return meta != nullptr ? meta->fixed_size() : 0;
    }

    bool TSLInputView::empty() const
    {
        return size() == 0;
    }

    TSInputView TSLInputView::at(std::size_t index) &
    {
        if (index >= size()) { throw std::out_of_range("TSLInputView::at index out of range"); }
        if (view_.is_target_position())
        {
            auto list = view_.target_view_.as_list();
            return view_.child_from_target(list.at(index), index);
        }
        if (view_.node_ == nullptr || view_.node_->role != TSEndpointRole::NonPeered)
        {
            throw std::logic_error("TSLInputView::at requires a non-peered list or bound target");
        }
        auto *child = index < view_.node_->children.size() ? view_.node_->children[index].get() : nullptr;
        if (child == nullptr) { throw std::logic_error("TSLInputView::at selected an unplanned input slot"); }
        return view_.child_from_node(child);
    }

    TSInputView TSLInputView::at(std::size_t index) const &
    {
        return const_cast<TSLInputView *>(this)->at(index);
    }

    TSInputView TSLInputView::operator[](std::size_t index) &
    {
        return at(index);
    }

    TSInputView TSLInputView::operator[](std::size_t index) const &
    {
        return at(index);
    }
}  // namespace hgraph

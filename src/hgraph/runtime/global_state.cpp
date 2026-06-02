#include <hgraph/runtime/global_state.h>

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/any_ops.h>
#include <hgraph/types/value/mutable_container_ops.h>

#include <string>

namespace hgraph
{
    namespace
    {
        // Canonical binding for the GlobalState backing: a mutable Map<string, Any>.
        const ValueTypeBinding &global_state_binding()
        {
            auto       &registry = TypeRegistry::instance();
            const auto *str_meta = registry.register_scalar<std::string>("str");
            const auto *any_meta = registry.any();
            const auto *schema   = registry.mutable_map(str_meta, any_meta);
            const auto *binding  = ValuePlanFactory::instance().binding_for(schema);
            if (binding == nullptr) { throw std::logic_error("GlobalState: no binding for Map<string, Any>"); }
            return *binding;
        }
    }  // namespace

    GlobalState::GlobalState() : map_{global_state_binding()} {}

    std::size_t GlobalStateView::size() const { return map_->as_map().size(); }

    bool GlobalStateView::contains(std::string_view key) const
    {
        const Value key_value{std::string{key}};
        return map_->as_map().contains(key_value.view());
    }

    ValueView GlobalStateView::get(std::string_view key) const
    {
        const Value key_value{std::string{key}};
        auto        view = map_->as_map();
        if (!view.contains(key_value.view())) { return ValueView{}; }
        return view.at(key_value.view()).as_any().get();
    }

    void GlobalStateView::set(std::string_view key, const ValueView &value) const
    {
        const Value key_value{std::string{key}};
        // Get (creating an empty Any if needed) the value slot and assign the
        // boxed value in place — a single copy of ``value``, no temporary Any.
        map_->as_map().begin_mutation().value(key_value.view()).as_mutable_any().set(value);
    }

    void GlobalStateView::set(std::string_view key, const Value &value) const { set(key, value.view()); }

    bool GlobalStateView::erase(std::string_view key) const
    {
        const Value key_value{std::string{key}};
        return map_->as_map().begin_mutation().remove(key_value.view());
    }
}  // namespace hgraph

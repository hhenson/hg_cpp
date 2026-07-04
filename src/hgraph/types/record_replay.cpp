#include <hgraph/types/record_replay.h>

#include <hgraph/runtime/graph.h>
#include <hgraph/types/primitive_types.h>

#include <stdexcept>
#include <vector>

namespace hgraph::record_replay
{
    namespace
    {
        // Wiring-time global state (single-threaded build; no thread-locals).
        // Reset through record_replay::reset(), chained from
        // OperatorRegistry::reset() so registry reset stays the single reset
        // point for all wiring-time global state.
        Config                  g_config{};
        std::vector<ScopeState> g_scopes{};
        const ScopeState        g_no_scope{};
    }  // namespace

    void set_config(Config config)
    {
        if (config.model.empty()) { throw std::invalid_argument("record/replay model must not be empty"); }
        if (config.date_key.empty() || config.as_of_key.empty())
        {
            throw std::invalid_argument("record/replay table keys must not be empty");
        }
        g_config = std::move(config);
    }

    const Config &config() { return g_config; }

    bool model_is(std::string_view model) noexcept { return g_config.model == model; }

    const ScopeState &current_scope() noexcept { return g_scopes.empty() ? g_no_scope : g_scopes.back(); }

    scope::scope(Mode mode, std::string recordable_id)
    {
        g_scopes.push_back(ScopeState{mode, std::move(recordable_id)});
    }

    scope::~scope()
    {
        if (!g_scopes.empty()) { g_scopes.pop_back(); }
    }

    void reset() noexcept
    {
        g_config = Config{};
        g_scopes.clear();
    }

    bool has_recordable_id(const GraphView &graph) noexcept
    {
        return graph.trait(RECORDABLE_ID_TRAIT).valid();
    }

    std::string fq_recordable_id(const GraphView &graph, std::string_view recordable_id)
    {
        const ValueView parent = graph.trait(RECORDABLE_ID_TRAIT);
        if (!parent.valid())
        {
            if (recordable_id.empty())
            {
                throw std::runtime_error("no recordable id provided and no parent recordable id trait found");
            }
            return std::string{recordable_id};
        }
        const Str &parent_id = parent.checked_as<Str>();
        if (recordable_id.empty()) { return parent_id; }
        return parent_id + "." + std::string{recordable_id};
    }
}  // namespace hgraph::record_replay

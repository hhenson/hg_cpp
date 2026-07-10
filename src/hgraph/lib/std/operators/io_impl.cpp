#include <hgraph/lib/std/operators/impl/io_impl.h>

namespace hgraph::stdlib
{
    void register_io_operators()
    {
        register_overload<debug_print, debug_print_impl>();
        register_overload<null_sink, null_sink_impl>();
        // ``record`` / ``replay`` register the in-memory GlobalState backend
        // (the testing toolkit's cycle-aligned ``List<Any>`` buffer) as the
        // operator implementations, making them name-resolvable — a Python
        // frontend needs the registry path, not the C++ ``wire<T>`` sugar
        // (tests/cpp/test_erased_wiring.cpp). Pluggable record/replay backends
        // (Python's model registry / DataWriter) remain roadmap P3.
        register_overload<record, testing::record>();
        register_overload<replay, testing::replay>();
        register_overload<log_, log_impl>();
        register_overload<print_sink_op, print_sink_impl>();
        register_graph_overload<print_, print_compose>();
        register_overload<assert_, assert_plain_impl>();
        register_overload<assert_fmt_op, assert_fmt_sink_impl>();
        register_graph_overload<assert_, assert_fmt_compose>();
    }

    IoWriteFn &io_write_slot() noexcept
    {
        static IoWriteFn writer = [](std::string_view line, bool to_stdout) {
            if (to_stdout) { fmt::print("{}\n", line); }
            else { fmt::print(stderr, "{}\n", line); }
        };
        return writer;
    }

    void io_write(std::string_view line, bool to_stdout) { io_write_slot()(line, to_stdout); }

    namespace io_impl_detail
    {
        std::string format_bundle(std::string_view format, const TSInputView &packed)
        {
            auto bundle = const_cast<TSInputView &>(packed).as_bundle();
            std::string result;
            result.reserve(format.size());
            std::size_t positional = 0;
            for (std::size_t i = 0; i < format.size(); ++i)
            {
                if (format[i] != '{')
                {
                    result.push_back(format[i]);
                    continue;
                }
                const auto close = format.find('}', i);
                if (close == std::string_view::npos)
                {
                    result.append(format.substr(i));
                    break;
                }
                const std::string_view name = format.substr(i + 1, close - i - 1);
                auto child = name.empty() ? bundle.at(positional++) : bundle.at(name);
                // A Str renders WITHOUT quoting (python print semantics).
                if (child.valid())
                {
                    const ValueView value = child.value();
                    if (const auto *text = value.try_as<Str>(); text != nullptr) { result.append(*text); }
                    else { result.append(value.to_string()); }
                }
                else { result.append("<n/a>"); }
                i = close;
            }
            return result;
        }

        WiringPortRef pack_format_args(std::vector<WiringPortRef> positional,
                                       std::vector<std::pair<std::string, WiringPortRef>> named)
        {
            std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
            std::vector<WiringPortRef> children;
            fields.reserve(positional.size() + named.size());
            children.reserve(positional.size() + named.size());
            std::size_t index = 0;
            for (WiringPortRef &port : positional)
            {
                fields.emplace_back(fmt::format("_{}", index++), port.schema);
                children.push_back(std::move(port));
            }
            for (auto &[name, port] : named)
            {
                fields.emplace_back(name, port.schema);
                children.push_back(std::move(port));
            }
            const auto *schema = TypeRegistry::instance().un_named_tsb(fields);
            return WiringPortRef::structural_source(schema, std::move(children));
        }
    }  // namespace io_impl_detail
}  // namespace hgraph::stdlib

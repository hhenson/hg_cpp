#include <hgraph/types/value/json_codec.h>

#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/util/date_time.h>

#include <fmt/core.h>

#include <hgraph/util/scope.h>

#include <charconv>
#include <chrono>
#include <locale>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <unordered_map>
#include <utility>

namespace hgraph
{
    namespace json_detail
    {
        // ---------------------------------------------------------------
        // Writing helpers
        // ---------------------------------------------------------------

        void append_escaped(std::string_view text, std::string &out)
        {
            out.push_back('"');
            for (const char c : text)
            {
                switch (c)
                {
                    case '"': out += "\\\""; break;
                    case '\\': out += "\\\\"; break;
                    case '\b': out += "\\b"; break;
                    case '\f': out += "\\f"; break;
                    case '\n': out += "\\n"; break;
                    case '\r': out += "\\r"; break;
                    case '\t': out += "\\t"; break;
                    default:
                        if (static_cast<unsigned char>(c) < 0x20)
                        {
                            out += fmt::format("\\u{:04x}", static_cast<unsigned char>(c));
                        }
                        else
                        {
                            out.push_back(c);
                        }
                }
            }
            out.push_back('"');
        }

        void append_date(Date value, std::string &out)
        {
            out += fmt::format("{:04}-{:02}-{:02}", static_cast<int>(value.year()),
                               static_cast<unsigned>(value.month()), static_cast<unsigned>(value.day()));
        }

        void append_time_of_day(std::int64_t micros_since_midnight, std::string &out)
        {
            const auto total_seconds = micros_since_midnight / 1'000'000;
            out += fmt::format("{:02}:{:02}:{:02}.{:06}", total_seconds / 3'600, (total_seconds / 60) % 60,
                               total_seconds % 60, micros_since_midnight % 1'000'000);
        }

        // ---------------------------------------------------------------
        // Reader — a minimal recursive-descent tokenizer. Parsing is
        // meta-directed: at every position the converter knows what shape it
        // expects, so no DOM is built.
        // ---------------------------------------------------------------

        struct Reader
        {
            std::string_view text;
            std::size_t      pos{0};

            [[noreturn]] void fail(std::string_view message) const
            {
                throw std::invalid_argument(fmt::format("from_json: {} at offset {}", message, pos));
            }

            void skip_ws() noexcept
            {
                while (pos < text.size())
                {
                    const char c = text[pos];
                    if (c != ' ' && c != '\t' && c != '\n' && c != '\r') { break; }
                    ++pos;
                }
            }

            [[nodiscard]] char peek()
            {
                skip_ws();
                if (pos >= text.size()) { fail("unexpected end of input"); }
                return text[pos];
            }

            void expect(char c)
            {
                if (peek() != c) { fail(fmt::format("expected '{}'", c)); }
                ++pos;
            }

            [[nodiscard]] bool consume_if(char c)
            {
                if (pos < text.size() && peek() == c)
                {
                    ++pos;
                    return true;
                }
                return false;
            }

            [[nodiscard]] bool consume_keyword(std::string_view keyword)
            {
                skip_ws();
                if (text.substr(pos, keyword.size()) == keyword)
                {
                    pos += keyword.size();
                    return true;
                }
                return false;
            }

            [[nodiscard]] std::string parse_string()
            {
                expect('"');
                std::string result;
                while (true)
                {
                    if (pos >= text.size()) { fail("unterminated string"); }
                    const char c = text[pos++];
                    if (c == '"') { return result; }
                    if (c != '\\')
                    {
                        result.push_back(c);
                        continue;
                    }
                    if (pos >= text.size()) { fail("unterminated escape"); }
                    const char e = text[pos++];
                    switch (e)
                    {
                        case '"': result.push_back('"'); break;
                        case '\\': result.push_back('\\'); break;
                        case '/': result.push_back('/'); break;
                        case 'b': result.push_back('\b'); break;
                        case 'f': result.push_back('\f'); break;
                        case 'n': result.push_back('\n'); break;
                        case 'r': result.push_back('\r'); break;
                        case 't': result.push_back('\t'); break;
                        case 'u': {
                            if (pos + 4 > text.size()) { fail("truncated \\u escape"); }
                            unsigned code = 0;
                            for (int i = 0; i < 4; ++i)
                            {
                                const char h = text[pos++];
                                code <<= 4;
                                if (h >= '0' && h <= '9') { code += static_cast<unsigned>(h - '0'); }
                                else if (h >= 'a' && h <= 'f') { code += static_cast<unsigned>(h - 'a' + 10); }
                                else if (h >= 'A' && h <= 'F') { code += static_cast<unsigned>(h - 'A' + 10); }
                                else { fail("bad \\u escape"); }
                            }
                            // UTF-8 encode (BMP only; surrogate pairs are not combined).
                            if (code < 0x80) { result.push_back(static_cast<char>(code)); }
                            else if (code < 0x800)
                            {
                                result.push_back(static_cast<char>(0xC0 | (code >> 6)));
                                result.push_back(static_cast<char>(0x80 | (code & 0x3F)));
                            }
                            else
                            {
                                result.push_back(static_cast<char>(0xE0 | (code >> 12)));
                                result.push_back(static_cast<char>(0x80 | ((code >> 6) & 0x3F)));
                                result.push_back(static_cast<char>(0x80 | (code & 0x3F)));
                            }
                            break;
                        }
                        default: fail("unknown escape");
                    }
                }
            }

            [[nodiscard]] std::string_view parse_number_token()
            {
                skip_ws();
                const std::size_t start = pos;
                while (pos < text.size())
                {
                    const char c = text[pos];
                    if ((c >= '0' && c <= '9') || c == '-' || c == '+' || c == '.' || c == 'e' || c == 'E')
                    {
                        ++pos;
                    }
                    else { break; }
                }
                if (pos == start) { fail("expected a number"); }
                return text.substr(start, pos - start);
            }

            /** Skip one complete JSON value (for unknown bundle fields). */
            void skip_value()
            {
                const char c = peek();
                switch (c)
                {
                    case '{': {
                        ++pos;
                        if (consume_if('}')) { return; }
                        while (true)
                        {
                            (void)parse_string();
                            expect(':');
                            skip_value();
                            if (!consume_if(',')) { break; }
                        }
                        expect('}');
                        return;
                    }
                    case '[': {
                        ++pos;
                        if (consume_if(']')) { return; }
                        while (true)
                        {
                            skip_value();
                            if (!consume_if(',')) { break; }
                        }
                        expect(']');
                        return;
                    }
                    case '"': (void)parse_string(); return;
                    case 't':
                        if (!consume_keyword("true")) { fail("bad literal"); }
                        return;
                    case 'f':
                        if (!consume_keyword("false")) { fail("bad literal"); }
                        return;
                    case 'n':
                        if (!consume_keyword("null")) { fail("bad literal"); }
                        return;
                    default: (void)parse_number_token(); return;
                }
            }
        };

        // ---------------------------------------------------------------
        // Atomic parse helpers (Python strptime-format compatible)
        // ---------------------------------------------------------------

        [[nodiscard]] std::int64_t parse_int_token(std::string_view token, const Reader &reader)
        {
            std::int64_t value{};
            const auto [ptr, ec] = std::from_chars(token.data(), token.data() + token.size(), value);
            if (ec != std::errc{} || ptr != token.data() + token.size())
            {
                const_cast<Reader &>(reader).fail("bad integer");
            }
            return value;
        }

        [[nodiscard]] double parse_float_token(std::string_view token, const Reader &reader)
        {
            double value{};
            std::istringstream stream{std::string{token}};
            stream.imbue(std::locale::classic());
            stream >> std::noskipws >> value;
            if (!stream || !stream.eof())
            {
                const_cast<Reader &>(reader).fail("bad number");
            }
            return value;
        }

        [[nodiscard]] std::int64_t parse_fixed_int(std::string_view text, std::size_t &pos, Reader &reader)
        {
            std::size_t start = pos;
            while (pos < text.size() && text[pos] >= '0' && text[pos] <= '9') { ++pos; }
            if (pos == start) { reader.fail("bad date/time component"); }
            std::int64_t value{};
            (void)std::from_chars(text.data() + start, text.data() + pos, value);
            return value;
        }

        void expect_char(std::string_view text, std::size_t &pos, char c, Reader &reader)
        {
            if (pos >= text.size() || text[pos] != c) { reader.fail("bad date/time separator"); }
            ++pos;
        }

        [[nodiscard]] Date parse_date_body(std::string_view s, std::size_t &i, Reader &reader)
        {
            const auto y = parse_fixed_int(s, i, reader);
            expect_char(s, i, '-', reader);
            const auto m = parse_fixed_int(s, i, reader);
            expect_char(s, i, '-', reader);
            const auto d = parse_fixed_int(s, i, reader);
            return Date{std::chrono::year{static_cast<int>(y)}, std::chrono::month{static_cast<unsigned>(m)},
                        std::chrono::day{static_cast<unsigned>(d)}};
        }

        [[nodiscard]] std::int64_t parse_time_body_micros(std::string_view s, std::size_t &i, Reader &reader)
        {
            const auto h = parse_fixed_int(s, i, reader);
            expect_char(s, i, ':', reader);
            const auto m = parse_fixed_int(s, i, reader);
            expect_char(s, i, ':', reader);
            const auto sec = parse_fixed_int(s, i, reader);
            std::int64_t micros = 0;
            if (i < s.size() && s[i] == '.')
            {
                ++i;
                const std::size_t start = i;
                micros                  = parse_fixed_int(s, i, reader);
                for (std::size_t digits = i - start; digits < 6; ++digits) { micros *= 10; }
            }
            return ((h * 60 + m) * 60 + sec) * 1'000'000 + micros;
        }
    }  // namespace json_detail

    namespace
    {
        using json_detail::Reader;
        using AtomicTag = JsonConverter::AtomicTag;

        // ---------------------------------------------------------------
        // Write thunks
        // ---------------------------------------------------------------

        void write_atomic(const JsonConverter &self, const ValueView &view, std::string &out)
        {
            switch (self.atomic_tag)
            {
                case AtomicTag::Bool: out += view.checked_as<Bool>() ? "true" : "false"; return;
                case AtomicTag::Int: out += fmt::format("{}", view.checked_as<Int>()); return;
                case AtomicTag::Float: out += fmt::format("{}", view.checked_as<Float>()); return;
                case AtomicTag::Str: json_detail::append_escaped(view.checked_as<Str>(), out); return;
                case AtomicTag::Date: {
                    out.push_back('"');
                    json_detail::append_date(view.checked_as<Date>(), out);
                    out.push_back('"');
                    return;
                }
                case AtomicTag::DateTime: {
                    const auto when = view.checked_as<DateTime>();
                    const auto day  = std::chrono::floor<std::chrono::days>(when);
                    out.push_back('"');
                    json_detail::append_date(Date{day}, out);
                    out.push_back(' ');
                    json_detail::append_time_of_day(
                        std::chrono::duration_cast<std::chrono::microseconds>(when - day).count(), out);
                    out.push_back('"');
                    return;
                }
                case AtomicTag::TimeDelta: {
                    // Python form: "days:hours:minutes:seconds.micros".
                    const auto delta  = view.checked_as<TimeDelta>();
                    const auto micros = delta.count();
                    const auto days   = micros / 86'400'000'000;
                    const auto rem    = micros % 86'400'000'000;
                    const auto secs   = rem / 1'000'000;
                    out += fmt::format("\"{}:{}:{}:{}.{:06}\"", days, secs / 3'600, (secs / 60) % 60, secs % 60,
                                       rem % 1'000'000);
                    return;
                }
                case AtomicTag::Time: {
                    out.push_back('"');
                    json_detail::append_time_of_day(view.checked_as<Time>().microseconds, out);
                    out.push_back('"');
                    return;
                }
                case AtomicTag::None: break;
            }
            throw std::logic_error("json: unsupported atomic write");
        }

        void write_composite(const JsonConverter &self, const ValueView &view, std::string &out)
        {
            // Bundle -> object; (un-named) tuple without field names -> array.
            const auto indexed  = view.as_indexed_view();
            const bool as_array = self.names.empty();
            out.push_back(as_array ? '[' : '{');
            bool first = true;
            for (std::size_t i = 0; i < self.children.size(); ++i)
            {
                const auto child = indexed.at(i);
                if (!as_array && !child.has_value()) { continue; }
                if (!std::exchange(first, false)) { out += ", "; }
                if (!as_array)
                {
                    json_detail::append_escaped(self.names[i], out);
                    out += ": ";
                }
                if (child.has_value()) { self.children[i]->write(child, out); }
                else { out += "null"; }
            }
            out.push_back(as_array ? ']' : '}');
        }

        void write_sequence(const JsonConverter &self, const ValueView &view, std::string &out)
        {
            out.push_back('[');
            bool first = true;
            if (view.schema()->kind == ValueTypeKind::List)
            {
                const auto list = view.as_list();
                for (std::size_t i = 0; i < list.size(); ++i)
                {
                    if (!std::exchange(first, false)) { out += ", "; }
                    self.children[0]->write(list.at(i), out);
                }
            }
            else
            {
                const auto set = view.as_set();
                for (const auto element : set)
                {
                    if (!std::exchange(first, false)) { out += ", "; }
                    self.children[0]->write(element, out);
                }
            }
            out.push_back(']');
        }

        void write_map(const JsonConverter &self, const ValueView &view, std::string &out)
        {
            const auto map = view.as_map();
            out.push_back('{');
            bool first = true;
            for (const auto [key, value] : map)
            {
                if (!std::exchange(first, false)) { out += ", "; }
                // A string-rendered key is used directly; other keys render
                // their token and are wrapped in quotes (the Python rule).
                std::string key_text;
                self.children[0]->write(key, key_text);
                if (!key_text.empty() && key_text.front() == '"') { out += key_text; }
                else { json_detail::append_escaped(key_text, out); }
                out += ": ";
                self.children[1]->write(value, out);
            }
            out.push_back('}');
        }

        // ---------------------------------------------------------------
        // Read thunks
        // ---------------------------------------------------------------

        Value read_atomic(const JsonConverter &self, Reader &reader)
        {
            switch (self.atomic_tag)
            {
                case AtomicTag::Bool: {
                    if (reader.consume_keyword("true")) { return Value{Bool{true}}; }
                    if (reader.consume_keyword("false")) { return Value{Bool{false}}; }
                    reader.fail("expected a boolean");
                }
                case AtomicTag::Int: return Value{json_detail::parse_int_token(reader.parse_number_token(), reader)};
                case AtomicTag::Float:
                    return Value{json_detail::parse_float_token(reader.parse_number_token(), reader)};
                case AtomicTag::Str: return Value{Str{reader.parse_string()}};
                case AtomicTag::Date: {
                    const std::string s = reader.parse_string();
                    std::size_t       i = 0;
                    return Value{json_detail::parse_date_body(s, i, reader)};
                }
                case AtomicTag::DateTime: {
                    const std::string s   = reader.parse_string();
                    std::size_t       i   = 0;
                    const Date        day = json_detail::parse_date_body(s, i, reader);
                    json_detail::expect_char(s, i, ' ', reader);
                    const auto micros = json_detail::parse_time_body_micros(s, i, reader);
                    return Value{DateTime{std::chrono::sys_days{day}.time_since_epoch() +
                                          std::chrono::microseconds{micros}}};
                }
                case AtomicTag::TimeDelta: {
                    const std::string s = reader.parse_string();
                    std::size_t       i = 0;
                    const auto        d = json_detail::parse_fixed_int(s, i, reader);
                    json_detail::expect_char(s, i, ':', reader);
                    const auto micros = json_detail::parse_time_body_micros(s, i, reader);
                    return Value{TimeDelta{d * 86'400'000'000 + micros}};
                }
                case AtomicTag::Time: {
                    const std::string s = reader.parse_string();
                    std::size_t       i = 0;
                    return Value{Time{json_detail::parse_time_body_micros(s, i, reader)}};
                }
                case AtomicTag::None: break;
            }
            throw std::logic_error("json: unsupported atomic read");
        }

        Value read_composite(const JsonConverter &self, Reader &reader)
        {
            BundleBuilder builder{*self.binding};
            if (self.names.empty())
            {
                reader.expect('[');
                for (std::size_t i = 0; i < self.children.size(); ++i)
                {
                    if (i != 0) { reader.expect(','); }
                    if (reader.consume_keyword("null"))
                    {
                        if (self.meta->kind != ValueTypeKind::Bundle)
                        {
                            reader.fail("null composite field is only supported for Bundle values");
                        }
                        continue;
                    }
                    builder.set(i, self.children[i]->read(reader));
                }
                reader.expect(']');
            }
            else
            {
                reader.expect('{');
                if (!reader.consume_if('}'))
                {
                    while (true)
                    {
                        const std::string key = reader.parse_string();
                        reader.expect(':');
                        std::size_t index = self.names.size();
                        for (std::size_t i = 0; i < self.names.size(); ++i)
                        {
                            if (self.names[i] == key)
                            {
                                index = i;
                                break;
                            }
                        }
                        if (index == self.names.size()) { reader.skip_value(); }
                        else if (reader.consume_keyword("null")) {}
                        else { builder.set(index, self.children[index]->read(reader)); }
                        if (!reader.consume_if(',')) { break; }
                    }
                    reader.expect('}');
                }
            }
            return builder.build();
        }

        Value read_list(const JsonConverter &self, Reader &reader)
        {
            ListBuilder builder{*self.children[0]->binding};
            reader.expect('[');
            if (!reader.consume_if(']'))
            {
                while (true)
                {
                    Value element = self.children[0]->read(reader);
                    builder.push_back_copy(element.view().data());
                    if (!reader.consume_if(',')) { break; }
                }
                reader.expect(']');
            }
            return builder.build();
        }

        Value read_set(const JsonConverter &self, Reader &reader)
        {
            SetBuilder builder{*self.children[0]->binding};
            reader.expect('[');
            if (!reader.consume_if(']'))
            {
                while (true)
                {
                    Value element = self.children[0]->read(reader);
                    (void)builder.insert_copy(element.view().data());
                    if (!reader.consume_if(',')) { break; }
                }
                reader.expect(']');
            }
            return builder.build();
        }

        Value read_map(const JsonConverter &self, Reader &reader)
        {
            MapBuilder builder{*self.children[0]->binding, *self.children[1]->binding};
            reader.expect('{');
            if (!reader.consume_if('}'))
            {
                while (true)
                {
                    // The key arrives as a JSON string; string-tagged keys use
                    // its content, other keys parse the content as their token.
                    const std::string key_text = reader.parse_string();
                    Value             key;
                    if (self.children[0]->atomic_tag == AtomicTag::Str) { key = Value{Str{key_text}}; }
                    else
                    {
                        Reader key_reader{std::string_view{key_text}};
                        // Quoted forms (dates etc.) arrive without their quotes;
                        // re-wrap so the atomic reader sees its expected shape.
                        std::string requoted;
                        if (self.children[0]->atomic_tag != AtomicTag::Int &&
                            self.children[0]->atomic_tag != AtomicTag::Float &&
                            self.children[0]->atomic_tag != AtomicTag::Bool)
                        {
                            requoted   = fmt::format("\"{}\"", key_text);
                            key_reader = Reader{std::string_view{requoted}};
                            key        = self.children[0]->read(key_reader);
                        }
                        else { key = self.children[0]->read(key_reader); }
                    }
                    reader.expect(':');
                    Value value = self.children[1]->read(reader);
                    builder.set_item_copy(key.view().data(), value.view().data());
                    if (!reader.consume_if(',')) { break; }
                }
                reader.expect('}');
            }
            return builder.build();
        }

        // ---------------------------------------------------------------
        // Synthesis + interning (cleared on registry reset)
        // ---------------------------------------------------------------

        // NO locks: wiring and evaluation are single-threaded (the
        // OperatorRegistry precedent) — push senders, the only cross-thread
        // entry, never touch converters.
        std::unordered_map<const ValueTypeMetaData *, std::unique_ptr<JsonConverter>> g_converters;

        [[nodiscard]] AtomicTag atomic_tag_for(const ValueTypeMetaData *meta)
        {
            if (meta == scalar_descriptor<Bool>::value_meta()) { return AtomicTag::Bool; }
            if (meta == scalar_descriptor<Int>::value_meta()) { return AtomicTag::Int; }
            if (meta == scalar_descriptor<Float>::value_meta()) { return AtomicTag::Float; }
            if (meta == scalar_descriptor<Str>::value_meta()) { return AtomicTag::Str; }
            if (meta == scalar_descriptor<Date>::value_meta()) { return AtomicTag::Date; }
            if (meta == scalar_descriptor<DateTime>::value_meta()) { return AtomicTag::DateTime; }
            if (meta == scalar_descriptor<TimeDelta>::value_meta()) { return AtomicTag::TimeDelta; }
            if (meta == scalar_descriptor<Time>::value_meta()) { return AtomicTag::Time; }
            return AtomicTag::None;
        }

        const JsonConverter *build_converter(const ValueTypeMetaData *meta);

        const JsonConverter *converter_for_locked(const ValueTypeMetaData *meta)
        {
            if (const auto it = g_converters.find(meta); it != g_converters.end()) { return it->second.get(); }
            return build_converter(meta);
        }

        const JsonConverter *build_converter(const ValueTypeMetaData *meta)
        {
            if (meta == nullptr) { throw std::logic_error("json: null value schema"); }

            auto converter     = std::make_unique<JsonConverter>();
            converter->meta    = meta;
            converter->binding = ValuePlanFactory::instance().binding_for(meta);
            auto *raw          = converter.get();
            // Insert before recursing so self-referential schemas terminate;
            // the guard removes the half-built entry if synthesis throws.
            g_converters.emplace(meta, std::move(converter));
            auto unwind = UnwindCleanupGuard([&] { g_converters.erase(meta); });

            switch (meta->kind)
            {
                case ValueTypeKind::Atomic: {
                    raw->atomic_tag = atomic_tag_for(meta);
                    if (raw->atomic_tag == AtomicTag::None)
                    {
                        throw std::logic_error(fmt::format("json: unsupported atomic scalar '{}'",
                                                           meta->display_name ? meta->display_name : "?"));
                    }
                    raw->write_ = &write_atomic;
                    raw->read_  = &read_atomic;
                    break;
                }
                case ValueTypeKind::Tuple:
                case ValueTypeKind::Bundle: {
                    for (std::size_t i = 0; i < meta->field_count; ++i)
                    {
                        raw->children.push_back(converter_for_locked(meta->fields[i].type));
                        if (meta->fields[i].name != nullptr) { raw->names.emplace_back(meta->fields[i].name); }
                    }
                    if (!raw->names.empty() && raw->names.size() != raw->children.size())
                    {
                        throw std::logic_error("json: partially-named composite is not supported");
                    }
                    raw->write_ = &write_composite;
                    raw->read_  = &read_composite;
                    break;
                }
                case ValueTypeKind::List: {
                    raw->children.push_back(converter_for_locked(meta->element_type));
                    raw->write_ = &write_sequence;
                    raw->read_  = &read_list;
                    break;
                }
                case ValueTypeKind::Set: {
                    raw->children.push_back(converter_for_locked(meta->element_type));
                    raw->write_ = &write_sequence;
                    raw->read_  = &read_set;
                    break;
                }
                case ValueTypeKind::Map: {
                    raw->children.push_back(converter_for_locked(meta->key_type));
                    raw->children.push_back(converter_for_locked(meta->element_type));
                    raw->write_ = &write_map;
                    raw->read_  = &read_map;
                    break;
                }
                default:
                    throw std::logic_error(fmt::format("json: unsupported value kind for '{}'",
                                                       meta->display_name ? meta->display_name : "?"));
            }
            unwind.release();
            return raw;
        }
    }  // namespace

    const JsonConverter &json_converter(const ValueTypeMetaData *meta)
    {
        // Composed + interned once per schema. Per-tick operator paths do NOT
        // call this: nodes resolve their converter in ``start`` and carry it
        // in node State (the lifecycle "compose once" contract); this lookup
        // serves wiring/start-time resolution and ad-hoc utility use.
        return *converter_for_locked(meta);
    }

    void clear_json_converters() noexcept { g_converters.clear(); }

    std::string to_json_string(const ValueView &view)
    {
        if (!view.valid()) { return "null"; }
        std::string out;
        json_converter(view.schema()).write(view, out);
        return out;
    }

    Value from_json_string(const JsonConverter &converter, std::string_view text)
    {
        json_detail::Reader reader{text};
        Value               result = converter.read(reader);
        reader.skip_ws();
        if (reader.pos != text.size()) { reader.fail("trailing content"); }
        return result;
    }

    Value from_json_string(const ValueTypeMetaData *meta, std::string_view text)
    {
        return from_json_string(json_converter(meta), text);
    }
}  // namespace hgraph

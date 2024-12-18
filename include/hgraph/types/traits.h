//
// Created by Howard Henson on 18/12/2024.
//

#ifndef TRAITS_H
#define TRAITS_H

#include <optional>
#include <unordered_map>
#include <string>
#include <any>

namespace hgraph
{
    struct Traits : nb::intrusive_base
    {
        using ptr = nb::ref<Traits>;

        Traits(Traits::ptr parent_traits);

        void set_traits(std::unordered_map<std::string, std::any> traits);

        [[nodiscard]] std::any& get_trait(const std::string &trait_name) const;

        [[nodiscard]] std::any& get_trait_or(const std::string &trait_name, std::any& def_value) const;

        [[nodiscard]] Traits::ptr copy() const;

    private:
        std::optional<Traits::ptr> _parent_traits;
        std::unordered_map<std::string, std::any> _traits;
    };
}

#endif  // TRAITS_H

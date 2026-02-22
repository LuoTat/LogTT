# 内置掩码规则(正则表达式, 替换字符串)
BUILTIN_MASKING: list[tuple[str, str]] = [
    (
        r"(?P<S>^|[^A-Za-z\d])([A-Za-z\d]{2,}:){3,}[A-Za-z\d]{2,}(?P<E>[^A-Za-z\d]|$)",
        r"$S<#ID#>$E",
    ),
    (
        r"(?P<S>^|[^A-Za-z\d])\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}(:\d{0,})?(?P<E>[^A-Za-z\d]|$)",
        r"$S<#IP#>$E",
    ),
    (
        r"(?P<S>^|[^A-Za-z\d])([A-Fa-f\d]{4,}\s){3,}[A-Fa-f\d]{4,}(?P<E>[^A-Za-z\d]|$)",
        r"$S<#SEQ#>$E",
    ),
    (
        r"(?P<S>^|[^A-Za-z\d])0x[A-Fa-f\d]+(?P<E>[^A-Za-z\d]|$)",
        r"$S<#HEX#>$E",
    ),
    (
        r"(?P<S>^|[^A-Za-z\d])[A-Fa-f\d]{4,}(?P<E>[^A-Za-z\d]|$)",
        r"$S<#HEX#>$E",
    ),
    (
        r"(?P<S>^|[^A-Za-z\d])[KMGT]?i?B(?P<E>[^A-Za-z\d]|$)",
        r"$S<#SIZE#>$E",
    ),
    (
        r"(?P<S>^|[^A-Za-z\d])(\d\d:)+\d\d(?P<E>[^A-Za-z\d]|$)",
        r"$S<#TIME#>$E",
    ),
    (
        r"(?P<S>^|[^A-Za-z\d])\d{1,3}(,\d\d\d)*(?P<E>[^A-Za-z\d]|$)",
        r"$S<#NUM#>$E",
    ),
    (
        r"(?P<S>^|[^A-Za-z\d])[-+]?\d+(?P<E>[^A-Za-z\d]|$)",
        r"$S<#NUM#>$E",
    ),
    # (
    #     r"(?P<S>^|[^A-Za-z\d])(([\w-]+\.){2,}[\w-]+)(?P<E>[^A-Za-z\d]|$)",
    #     "<FQDN>",
    # ),
]

from typing import Any, TypeVar

_T = TypeVar("_T", bound=JITMixin)

def jit_mixin(obj: object, mixin: type[_T]) -> _T: ...
# `type[Any]`: the generated classes mix arbitrary bases, any attribute
# may be accessed on them
def jit_mixin_type(base: type, *mixins: type[JITMixin]) -> type[Any]: ...  # pyright: ignore[reportExplicitAny]

_mixin_cache: dict[tuple[type, tuple[type[JITMixin], ...]], type[Any]]  # pyright: ignore[reportExplicitAny]

class JITMixin:
    # Set on mixin classes (prefix) and on generated classes (base, mixins)
    _jit_mixin_prefix: str
    _jit_mixin_base: type
    _jit_mixins: tuple[type[JITMixin], ...]

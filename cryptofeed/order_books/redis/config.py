import os


REDIS_URI = 'redis://localhost'

_ABS_PATH = os.path.abspath(os.path.dirname(__file__))
LUA_SCRIPT_PATHS = (
    os.path.join(_ABS_PATH, './lua_scripts/delete_if_zero_size.lua'),
    os.path.join(_ABS_PATH, './lua_scripts/incr_if_exists.lua'),
    os.path.join(_ABS_PATH, './lua_scripts/incr_if_exists_else_set_abs.lua'),
    os.path.join(_ABS_PATH, './lua_scripts/decr_and_remove_if_zero.lua'),
    os.path.join(_ABS_PATH, './lua_scripts/get_pair_book.lua'),
)

def load_redis_script(file_path: str, pool, loop)-> None:
    """
    :param file_path: str-> path to lua script file
    :param pool: aioredis.pool.ConnectionsPool-> async redis connections pool
    :param loop: asyncio.EventLoop-> event loop ot None
    :return: None
    """
    file_name = file_path.split('/')[-1]
    func_name = file_name.split('.')[0]
    script_key = 'script:{}'.format(func_name)
    # check if script is loaded
    script_hash = loop.run_until_complete(pool.get(script_key))
    if not script_hash:
        # load script and get hash
        with open(file_path, 'r') as script_file:
            script_hash = loop.run_until_complete(
                pool.script_load(script_file.read())
            )
        # save lua script hash in key 'script:{script_name}'
        loop.run_until_complete(pool.set(script_key, script_hash))
    setattr(pool, func_name, script_hash)

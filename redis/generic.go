package redis

func (rdb *RedisDB) Del(key []byte) error {
	return rdb.db.Delete(key)
}

func (rdb *RedisDB) Type(key []byte) redisDataStructureType {
	encValue, err := rdb.db.Get(key)
	if err != nil {
		return RUnknown
	}

	if len(encValue) == 0 {
		return RUnknown
	}
	return redisDataStructureType(encValue[0])
}

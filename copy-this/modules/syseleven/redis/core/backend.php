<?php
/**
 * Redis adapter for Zend_Cache
 *
 * forked from https://github.com/colinmollenhour/Cm_Cache_Backend_Redis
 *
 * @copyright  Copyright (c) 2013 Colin Mollenhour (http://colin.mollenhour.com)
 * @license    http://framework.zend.com/license/new-bsd     New BSD License
 *
 * @author     Colin Mollenhour (http://colin.mollenhour.com)
 * @author     Daniel Niedergesäß (http://syseleven.de)
 */
class SysEleven_Cache_Backend_Redis{

    const SET_IDS         = 'zc:ids';
    const SET_TAGS        = 'zc:tags';

    const PREFIX_KEY      = 'zc:k:';
    const PREFIX_TAG_IDS  = 'zc:ti:';

    const FIELD_DATA      = 'd';
    const FIELD_MTIME     = 'm';
    const FIELD_TAGS      = 't';
    const FIELD_INF       = 'i';

    const MAX_LIFETIME    = 2592000; /* Redis backend limit */

    const COMPRESS_PREFIX = ":\x1f\x8b";

    const COMPRESS_PREFIX_SN = "sn:\x1f\x8b";
    const COMPRESS_PREFIX_GZ = "gz:\x1f\x8b";
    const COMPRESS_PREFIX_ZC = "zc:";

    const DEFAULT_CONNECT_TIMEOUT = 1.5;
    const DEFAULT_CONNECT_RETRIES = 10;

    /**
     * instance of redis client
     *
     * @var Redis */
    protected $_redis;

    /**
     *
     *
     * @var bool
     */
    protected $_notMatchingTags = false;

    /**
     * max lifetime for a cachekey limited by redis server
     *
     * @var int
     */
    protected $_lifetimelimit = self::MAX_LIFETIME;

    /**
     * compression level for tags
     *
     * @var int
     */
    protected $_compressTags = 6;

    /**
     * compression level for data
     *
     * @var int
     */
    protected $_compressData = 6;

    /**
     * string lenght for compression
     *
     * @var int
     */
    protected $_compressThreshold = 1024;

    /**
     * used compression libary
     *
     * @var string
     */
    protected $_compressionLib;

    /**
     * persistent cache tags
     *
     * @var array
     */
    protected $_persistentCacheTags = array();

    /**
     * current unix time
     *
     * @var int
     */
    protected $_time = null;


    /**
     * Contruct Zend_Cache Redis backend
     * @param array $options
     * @return \SysEleven_Cache_Backend_Redis
     */
    public function __construct($options = array())
    {
        if ( empty($options['server']) === true ) {
            Zend_Cache::throwException('Redis "server" not specified.');
        }

        if ( empty($options['port']) === true && substr($options['server'],0,1) !== '/' ) {
            Zend_Cache::throwException('Redis "port" not specified.');
        }

        //create redis instance
        $this->_redis = new Redis();

        $timeout = isset($options['timeout']) ? $options['timeout'] : self::DEFAULT_CONNECT_TIMEOUT;
        $persistent = isset($options['persistent']) ? $options['persistent'] : '';
        //prepare tries for connect
        $connectRetries = isset($options['connect_retries']) ? (int)$options['connect_retries'] : self::DEFAULT_CONNECT_RETRIES;
        $connectRetriesCount = 0;
        $isConnected = false;
        //try to connect
        while($isConnected === false && $connectRetriesCount < $connectRetries){
            try {
                //create connection
                if (isset($options['persistent']) === true && strlen($options['persistent']) > 0) {
                    $this->_redis->pconnect($options['server'], $options['port'], $timeout, $persistent);
                } else {
                    $this->_redis->connect($options['server'], $options['port'], $timeout);
                }
                //try login with password credentials
                if ( empty($options['password']) === false) {
                    if($this->_redis->auth($options['password']) === false){
                        Zend_Cache::throwException('Unable to authenticate with the redis server.');
                    }
                }
                $isConnected = true;
            } catch (RedisException $e) {
                if($connectRetriesCount >= $connectRetries){
                    Zend_Cache::throwException('Unable to connect the redis server after '.$connectRetries.' tries.');
                }
                $connectRetriesCount++;
            }
        }

        //set read timeout. if not set default from php.ini is taken
        if ( empty($options['read_timeout']) === false && (float)$options['read_timeout'] > 0) {
            $this->_redis->setOption(Redis::OPT_READ_TIMEOUT, (float) $options['read_timeout']);ini_get('default_socket_timeout');
        }else{
            $this->_redis->setOption(Redis::OPT_READ_TIMEOUT, (float) ini_get('default_socket_timeout'));
        }

        if ( isset($options['notMatchingTags']) ) {
            $this->_notMatchingTags = (bool) $options['notMatchingTags'];
        }

        if ( isset($options['compress_tags'])) {
            $this->_compressTags = (int) $options['compress_tags'];
        }

        if ( isset($options['compress_data'])) {
            $this->_compressData = (int) $options['compress_data'];
        }

        if ( isset($options['lifetimelimit'])) {
            $this->_lifetimelimit = (int) min($options['lifetimelimit'], self::MAX_LIFETIME);
        }

        if ( isset($options['compress_threshold'])) {
            $this->_compressThreshold = (int) $options['compress_threshold'];
        }

        if ( isset($options['automatic_cleaning_factor']) ) {
            $this->_options['automatic_cleaning_factor'] = (int) $options['automatic_cleaning_factor'];
        } else {
            $this->_options['automatic_cleaning_factor'] = 0;
        }
        switch (true) {
            case (isset($options['compression_lib']) === true && $options['compression_lib'] == 'snappy'):
            {
                if (function_exists('snappy_compress')) {
                    $this->_compressionLib = 'snappy';
                    $this->_compressPrefix = self::COMPRESS_PREFIX_SN;
                } else {
                    Zend_Cache::throwException('Unable to compress data. Snappy compression module not loaded.');
                }
                break;
            }
            case (isset($options['compression_lib']) === true && $options['compression_lib'] == 'gzip'):
            default:
                {
                if (function_exists('gzcompress')) {
                    $this->_compressionLib = 'gzip';
                    $this->_compressPrefix = self::COMPRESS_PREFIX_GZ;
                }else{
                    Zend_Cache::throwException('Unable to compress data. Gzip compression module not loaded.');
                }
                break;
                }

        }

        //proceed persistent cache tags
        if (isset($options['persistent_cache_tags'])) {
            $id_prefix = (string)Mage::getConfig()->getNode('global/cache/prefix');
            if (empty($id_prefix) === true) {
                Zend_Cache::throwException('Persistent cache tags are defined but id_prefix is not set');
            }
            $this->_persistentCacheTags = array();
            foreach ($options['persistent_cache_tags'] as $tag) {
                $this->_persistentCacheTags[] = $id_prefix . $tag;
            }
        } else {
            $this->_persistentCacheTags = array();
        }
    }

    /**
     * Load value with given id from cache
     *
     * @param  string  $id                     Cache id
     * @param  boolean $doNotTestCacheValidity If set to true, the cache validity won't be tested
     * @return bool|string
     */
    public function load($id, $doNotTestCacheValidity = false)
    {
        $data = $this->_redis->hGet(self::PREFIX_KEY.$id, self::FIELD_DATA);
        if ($data === null) {
            return false;
        }
        return $this->_decodeData($data);
    }

    /**
     * Test if a cache is available or not (for the given id)
     *
     * @param  string $id Cache id
     * @return bool|int False if record is not available or "last modified" timestamp of the available cache record
     */
    public function test($id)
    {
        $mtime = $this->_redis->hGet(self::PREFIX_KEY.$id, self::FIELD_MTIME);
        return ($mtime ? $mtime : false);
    }

    /**
     * Save some string datas into a cache record
     *
     * Note : $data is always "string" (serialization is done by the
     * core not by the backend)
     *
     * @param  string $data             Datas to cache
     * @param  string $id               Cache id
     * @param  array  $tags             Array of strings, the cache record will be tagged by each string entry
     * @param  bool|int $specificLifetime If != false, set a specific lifetime for this cache record (null => infinite lifetime)
     * @return boolean True if no problem
     */
    public function save($data, $id, $tags = array(), $specificLifetime = false)
    {
        if ( ! is_array($tags)) $tags = $tags ? array($tags) : array();

        $lifetime = $this->getLifetime($specificLifetime);

        // Get list of tags previously assigned
        $oldTags = $this->_decodeData($this->_redis->hGet(self::PREFIX_KEY.$id, self::FIELD_TAGS));
        $oldTags = $oldTags ? explode(',', $oldTags) : array();

        $this->_redis->multi();

        // Set the data
        $result = $this->_redis->hMSet(self::PREFIX_KEY.$id, array(
            self::FIELD_DATA => $this->_encodeData($data, $this->_compressData),
            self::FIELD_TAGS => $this->_encodeData(implode(',',$tags), $this->_compressTags),
            self::FIELD_MTIME => $this->_getTime(),
            self::FIELD_INF => $lifetime ? 0 : 1,
        ));

        //run code only cache key was stored
        if( $result !== true) {

            if($this->_isCachePersistent($tags)){
                //store key persistent
                $this->_redis->persist(self::PREFIX_KEY.$id);
            } else {
                // Always expire so the volatile-* eviction policies may be safely used, otherwise
                // there is a risk that tag data could be evicted. Check if given cache tags are persistent
                // caches so we set them as persistent
                $this->_redis->expire(self::PREFIX_KEY.$id, $lifetime ? $lifetime : $this->_lifetimelimit);
            }

            // Process added tags
            if ($tags)
            {
                // Update the list with all the tags
                // Update the id list for each tag
                foreach($tags as $tag)
                {
                    $this->_redis->sAdd(self::SET_TAGS, $tag);
                    $this->_redis->sAdd(self::PREFIX_TAG_IDS . $tag, $id);
                }
            }

            // Process removed tags
            if ($remTags = ($oldTags ? array_diff($oldTags, $tags) : false))
            {
                // Update the id list for each tag
                foreach($remTags as $tag)
                {
                    $this->_redis->sRem(self::PREFIX_TAG_IDS . $tag, $id);
                }
            }

            // Update the list with all the ids
            if($this->_notMatchingTags) {
                $this->_redis->sAdd(self::SET_IDS, $id);
            }

            $this->_redis->exec();
        }else{
            $this->_redis->discard();
        }
        return true;
    }

    /**
     * Remove a cache record
     *
     * @param  string $id Cache id
     * @return boolean True if no problem
     */
    public function remove($id)
    {
        // Get list of tags for this id
        $tags = explode(',', $this->_decodeData($this->_redis->hGet(self::PREFIX_KEY.$id, self::FIELD_TAGS)));

        $this->_redis->multi();

        // Remove data
        $this->_redis->del(self::PREFIX_KEY.$id);

        // Remove id from list of all ids
        if($this->_notMatchingTags) {
            $this->_redis->sRem( self::SET_IDS, $id );
        }

        // Update the id list for each tag
        foreach($tags as $tag) {
            $this->_redis->sRem(self::PREFIX_TAG_IDS . $tag, $id);
        }

        $result = $this->_redis->exec();

        return (bool) $result[0];
    }

    /**
     * @param array $tags
     */
    protected function _removeByNotMatchingTags($tags)
    {
        $ids = $this->getIdsNotMatchingTags($tags);
        if($ids)
        {
            $this->_redis->multi();
            // Remove data
            $this->_redis->del( $this->_preprocessIds($ids));

            // Remove ids from list of all ids
            if($this->_notMatchingTags) {
                $this->_redis->sRem( self::SET_IDS, $ids);
            }
            $this->_redis->exec();
        }
    }

    /**
     * flush database
     */
    public function flushDB(){
        $this->_redis->flushDB();
    }
    /**
     * @param array $tags
     */
    protected function _removeByMatchingTags($tags)
    {
        $ids = $this->getIdsMatchingTags($tags);
        if($ids)
        {
            $this->_redis->multi();

            // Remove data
            $this->_redis->del( $this->_preprocessIds($ids));

            // Remove ids from list of all ids
            if($this->_notMatchingTags) {
                $this->_redis->sRem( self::SET_IDS, $ids);
            }

            $this->_redis->exec();
        }
    }

    /**
     * @param array $tags
     */
    protected function _removeByMatchingAnyTags($tags)
    {
        $ids = $this->getIdsMatchingAnyTags($tags);

        $this->_redis->multi();

        if($ids)
        {
            // Remove data
            $this->_redis->del( $this->_preprocessIds($ids));

            // Remove ids from list of all ids
            if($this->_notMatchingTags) {
                $this->_redis->sRem( self::SET_IDS, $ids);
            }
        }

        // Remove tag id lists
        $this->_redis->del( $this->_preprocessTagIds($tags));

        // Remove tags from list of tags
        $this->_redis->sRem( self::SET_TAGS, $tags);

        $this->_redis->exec();
    }

    /**
     * Clean up tag id lists since as keys expire the ids remain in the tag id lists
     */
    protected function _collectGarbage()
    {
        // Clean up expired keys from tag id set and global id set
        $exists = array();
        $tags = (array) $this->_redis->sMembers(self::SET_TAGS);
        foreach($tags as $tag)
        {
            // Get list of expired ids for each tag
            $tagMembers = $this->_redis->sMembers(self::PREFIX_TAG_IDS . $tag);
            $numTagMembers = count($tagMembers);
            $expired = array();
            $numExpired = $numNotExpired = 0;
            if($numTagMembers) {
                while ($id = array_pop($tagMembers)) {
                    if( ! isset($exists[$id])) {
                        $exists[$id] = $this->_redis->exists(self::PREFIX_KEY.$id);
                    }
                    if ($exists[$id]) {
                        $numNotExpired++;
                    }
                    else {
                        $numExpired++;
                        $expired[] = $id;
                        // Remove incrementally to reduce memory usage
                        if (count($expired) % 100 == 0 && $numNotExpired > 0) {

                            $this->_redis->sRem( self::PREFIX_TAG_IDS . $tag, $expired);
                            if($this->_notMatchingTags) { // Clean up expired ids from ids set
                                $this->_redis->sRem( self::SET_IDS, $expired);
                            }
                            $expired = array();
                        }
                    }
                }
                if( empty($expired)) continue;
            }
            // Remove empty tags or completely expired tags
            if ($numExpired == $numTagMembers) {
                $this->_redis->del(self::PREFIX_TAG_IDS . $tag);
                $this->_redis->sRem(self::SET_TAGS, $tag);
            }
            // Clean up expired ids from tag ids set
            else if (false === empty($expired)) {
                $this->_redis->sRem( self::PREFIX_TAG_IDS . $tag, $expired);
                if($this->_notMatchingTags) { // Clean up expired ids from ids set
                    $this->_redis->sRem( self::SET_IDS, $expired);
                }
            }
            unset($expired);
        }

        // Clean up global list of ids for ids with no tag
        if($this->_notMatchingTags) {
            // TODO
        }
    }

    /**
     * Clean some cache records
     *
     * Available modes are :
     * 'all' (default)  => remove all cache entries ($tags is not used)
     * 'old'            => runs _collectGarbage()
     * 'matchingTag'    => supported
     * 'notMatchingTag' => supported
     * 'matchingAnyTag' => supported
     *
     * @param  string $mode Clean mode
     * @param  array  $tags Array of tags
     * @throws Zend_Cache_Exception
     * @return boolean True if no problem
     */
    public function clean($mode = oxcacheredis::CLEANING_MODE_ALL, $tags = array())
    {
        if( $tags && ! is_array($tags)) {
            $tags = array($tags);
        }

        if($mode == oxcacheredis::CLEANING_MODE_ALL) {
            return $this->_redis->flushDb();
        }

        if($mode == oxcacheredis::CLEANING_MODE_OLD) {
            $this->_collectGarbage();
            return true;
        }

        if( empty($tags)) {
            return true;
        }

        switch ($mode)
        {
            case oxcacheredis::CLEANING_MODE_MATCHING_TAG:

                $this->_removeByMatchingTags($tags);
                break;

            case oxcacheredis::CLEANING_MODE_NOT_MATCHING_TAG:

                $this->_removeByNotMatchingTags($tags);
                break;

            case oxcacheredis::CLEANING_MODE_MATCHING_ANY_TAG:

                $this->_removeByMatchingAnyTags($tags);
                break;

            default:
                Zend_Cache::throwException('Invalid mode for clean() method: '.$mode);
        }
        return true;
    }

    /**
     * Return true if the automatic cleaning is available for the backend
     *
     * @return boolean
     */
    public function isAutomaticCleaningAvailable()
    {
        return true;
    }

    /**
     * Set the frontend directives
     *
     * @param  array $directives Assoc of directives
     * @throws Zend_Cache_Exception
     * @return void
     */
    public function setDirectives($directives)
    {
        parent::setDirectives($directives);
        $lifetime = $this->getLifetime(false);
        if ($lifetime > self::MAX_LIFETIME) {
            Zend_Cache::throwException('Redis backend has a limit of 30 days (2592000 seconds) for the lifetime');
        }
    }

    /**
     * Return an array of stored cache ids
     *
     * @return array array of stored cache ids (string)
     */
    public function getIds()
    {
        if($this->_notMatchingTags) {
            return (array) $this->_redis->sMembers(self::SET_IDS);
        } else {
            $keys = $this->_redis->keys(self::PREFIX_KEY . '*');
            $prefixLen = strlen(self::PREFIX_KEY);
            foreach($keys as $index => $key) {
                $keys[$index] = substr($key, $prefixLen);
            }
            return $keys;
        }
    }

    /**
     * Return an array of stored tags
     *
     * @return array array of stored tags (string)
     */
    public function getTags()
    {
        return (array) $this->_redis->sMembers(self::SET_TAGS);
    }

    /**
     * Return an array of stored cache ids which match given tags
     *
     * In case of multiple tags, a logical AND is made between tags
     *
     * @param array $tags array of tags
     * @return array array of matching cache ids (string)
     */
    public function getIdsMatchingTags($tags = array())
    {
        if ($tags) {
            return (array) $this->_redis->sInter( $this->_preprocessTagIds($tags) );
        }
        return array();
    }

    /**
     * Return an array of stored cache ids which don't match given tags
     *
     * In case of multiple tags, a negated logical AND is made between tags
     *
     * @param array $tags array of tags
     * @return array array of not matching cache ids (string)
     */
    public function getIdsNotMatchingTags($tags = array())
    {
        if( ! $this->_notMatchingTags) {
            Zend_Cache::throwException("notMatchingTags is currently disabled.");
        }
        if ($tags) {
            return (array) $this->_redis->sDiff( array_merge(array(self::SET_IDS), $this->_preprocessTagIds($tags) ));
        }
        return (array) $this->_redis->sMembers( self::SET_IDS );
    }

    /**
     * Return an array of stored cache ids which match any given tags
     *
     * In case of multiple tags, a logical OR is made between tags
     *
     * @param array $tags array of tags
     * @return array array of any matching cache ids (string)
     */
    public function getIdsMatchingAnyTags($tags = array())
    {
        if ($tags) {
            return (array) $this->_redis->sUnion( $this->_preprocessTagIds($tags));
        }
        return array();
    }

    /**
     * Return the filling percentage of the backend storage
     *
     * @throws Zend_Cache_Exception
     * @return int integer between 0 and 100
     */
    public function getFillingPercentage()
    {
        return 0;
    }

    /**
     * Return an array of metadatas for the given cache id
     *
     * The array must include these keys :
     * - expire : the expire timestamp
     * - tags : a string array of tags
     * - mtime : timestamp of last modification time
     *
     * @param string $id cache id
     * @return array array of metadatas (false if the cache id is not found)
     */
    public function getMetadatas($id)
    {
        list($tags, $mtime, $inf) = $this->_redis->hMGet(self::PREFIX_KEY.$id, array(self::FIELD_TAGS, self::FIELD_MTIME, self::FIELD_INF));
        if( ! $mtime) {
            return false;
        }
        $tags = explode(',', $this->_decodeData($tags));
        $expire = $inf === '1' ? false : $this->_getTime() + $this->_redis->ttl(self::PREFIX_KEY.$id);

        return array(
            'expire' => $expire,
            'tags'   => $tags,
            'mtime'  => $mtime,
        );
    }

    /**
     * Give (if possible) an extra lifetime to the given cache id
     *
     * @param string $id cache id
     * @param int $extraLifetime
     * @return boolean true if ok
     */
    public function touch($id, $extraLifetime)
    {
        list($inf) = $this->_redis->hGet(self::PREFIX_KEY.$id, self::FIELD_INF);
        if ($inf === '0') {
            $expireAt = $this->_getTime() + $this->_redis->ttl(self::PREFIX_KEY.$id) + $extraLifetime;
            return (bool) $this->_redis->expireAt(self::PREFIX_KEY.$id, $expireAt);
        }
        return false;
    }

    /**
     * Return an associative array of capabilities (booleans) of the backend
     *
     * The array must include these keys :
     * - automatic_cleaning (is automating cleaning necessary)
     * - tags (are tags supported)
     * - expired_read (is it possible to read expired cache records
     *                 (for doNotTestCacheValidity option for example))
     * - priority does the backend deal with priority when saving
     * - infinite_lifetime (is infinite lifetime can work with this backend)
     * - get_list (is it possible to get the list of cache ids and the complete list of tags)
     *
     * @return array associative of with capabilities
     */
    public function getCapabilities()
    {
        return array(
            'automatic_cleaning' => ($this->_options['automatic_cleaning_factor'] > 0),
            'tags'               => true,
            'expired_read'       => false,
            'priority'           => false,
            'infinite_lifetime'  => true,
            'get_list'           => true,
        );
    }

    /**
     * @param string $data
     * @param int $level
     * @throws Zend_Cache_Exception
     * @return string
     */
    protected function _encodeData($data, $level)
    {
        if (isset($data[$this->_compressThreshold]) === true){
            return $data;
        }
        if ($level) {
            switch($this->_compressionLib) {
                case 'snappy': $data = snappy_compress($data); break;
                case 'gzip':   $data = gzcompress($data, $level); break;
            }
            if( $data === false) {
                Zend_Cache::throwException('Could not compress cache data.');
            }
            return $this->_compressPrefix.$data;
        }
        return $data;
    }

    /**
     * @param bool|string $data
     * @return string
     */
    protected function _decodeData($data)
    {
        switch (substr($data, 0, 5)) {
            case self::COMPRESS_PREFIX_SN:
                return snappy_uncompress(substr($data, 5));
            case self::COMPRESS_PREFIX_GZ:
            case self::COMPRESS_PREFIX_ZC:
                return gzuncompress(substr($data, 5));
        }
        return $data;
    }

    /**
     * @param $item
     * @param $index
     * @param $prefix
     */
    protected function _preprocess(&$item, $index, $prefix)
    {
        $item = $prefix . $item;
    }

    /**
     * @param $ids
     * @return array
     */
    protected function _preprocessIds($ids)
    {
        array_walk($ids, array($this, '_preprocess'), self::PREFIX_KEY);
        return $ids;
    }

    /**
     * @param $tags
     * @return array
     */
    protected function _preprocessTagIds($tags)
    {
        array_walk($tags, array($this, '_preprocess'), self::PREFIX_TAG_IDS);
        return $tags;
    }

    /**
     * Required to pass unit tests
     *
     * @param  string $id
     * @return void
     */
    public function ___expire($id)
    {
        $this->_redis->delete(self::PREFIX_KEY.$id);
    }

    /**
     * Check if one of the given cache tags is defined
     * as persistent cache tag
     *
     * @param array $tags
     * @return boolean
     */
    protected function _isCachePersistent(array $tags){
        if(false === empty($tags) && false === empty($this->_persistentCacheTags)){
            foreach($this->_persistentCacheTags as $tag){
                if(isset($tags[$tag]) === true){
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * used to make only time() request
     *
     * @return int
     */
    private function _getTime()
    {
        if (null === $this->_time){
            $this->_time = time();
        }

        return $this->_time;
    }

    /**
     * Get the life time
     *
     * if $specificLifetime is not false, the given specific life time is used
     * else, the global lifetime is used
     *
     * @param  int $specificLifetime
     * @return int Cache life time
     */
    public function getLifetime($specificLifetime)
    {
        if ($specificLifetime === false) {
            return $this->_directives['lifetime'];
        }
        return $specificLifetime;
    }
}
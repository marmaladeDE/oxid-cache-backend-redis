<?php
final class syseleven_redis_core_oxutils extends syseleven_redis_core_oxutils_parent
{
    /**
     * @var null|SysEleven_Cache_Backend_Redis
     */
    protected $oRedis = null;

    public function __construct()
    {
        require_once(dirname(__FILE__) . '/backend.php');
        $config = $this->getConfig()->getConfigParam('oxcacheredis');
        $this->oRedis = new SysEleven_Cache_Backend_Redis($config);
        parent::__construct();
    }

    /**
     * Adds contents to cache contents by given key. Returns true on success.
     * All file caches are supposed to be written once by commitFileCache() method.
     * @param string $sKey      Cache key
     * @param mixed $mContents Contents to cache
     * @return bool
     */
    public function toFileCache($sKey, $mContents)
    {
        $this->oRedis->save(serialize($mContents), $sKey, array(), 3600);
        $this->_aFileCacheContents[$sKey] = $mContents;
        $this->logger($sKey);
        $this->logger($mContents);
        #var_dump($sKey);
        #$this->_aFileCacheContents[$sKey] = $mContents;
        //$aMeta = $this->getCacheMeta($sKey);
        //var_dump($aMeta);
        // looking for cache meta
        //$sCachePath = isset($aMeta["cachepath"]) ? $aMeta["cachepath"] : $this->getCacheFilePath($sKey);
        //return ( bool )$this->_lockFile($sCachePath, $sKey);
    }


    /**
     * Fetches contents from file cache.
     * @param string $sKey Cache key
     * @return mixed
     */
    public function fromFileCache($sKey)
    {
        //try to fetch from array cache
        if (false === array_key_exists($sKey, $this->_aFileCacheContents)) {

            $sRes = $this->oRedis->load($sKey);
            if ($sRes !== false) {
                $sRes = unserialize($sRes);
                if($sRes !== false){
                    $this->logger('HIT: '.$sKey);
                    $this->_aFileCacheContents[$sKey] = $sRes;
                }
            }else{
                $this->logger('MISS: '.$sKey);
            }
        }
        return $this->_aFileCacheContents[$sKey];
    }

}
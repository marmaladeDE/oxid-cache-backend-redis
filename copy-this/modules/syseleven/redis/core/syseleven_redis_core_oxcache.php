<?php
/**
 * Created by JetBrains PhpStorm.
 * User: dng
 * Date: 29.05.13
 * Time: 11:52
 * To change this template use File | Settings | File Templates.
 */

class syseleven_redis_core_oxcache extends syseleven_redis_core_oxcache_parent{


    const CLEANING_MODE_ALL              = 'all';
    const CLEANING_MODE_OLD              = 'old';
    const CLEANING_MODE_MATCHING_TAG     = 'matchingTag';
    const CLEANING_MODE_NOT_MATCHING_TAG = 'notMatchingTag';
    const CLEANING_MODE_MATCHING_ANY_TAG = 'matchingAnyTag';

    /**
     * @var SysEleven_Cache_Backend_Redis
     */
    private $oRedis = null;

    public function __construct(){
        require_once(dirname(__FILE__) . '/backend.php');
        $config = $this->getConfig()->getConfigParam('oxcacheredis');
        $this->oRedis = new SysEleven_Cache_Backend_Redis($config);
        parent::__construct();
    }

    /**
     * Adds an ID and a content to the cache.
     *
     * @param string $sCacheId a cache ID
     * @param string $sContent a content
     * @param string $sResetOn a reset (date?)
     *
     * @return null
     */
    public function put( $sCacheId, $sContent, $sResetOn = '' )
    {
        #var_dump('put:'.$sCacheId,$sResetOn);
        #var_dump('put-tags:'.$sResetOn);
        #var_dump(explode('|',$sResetOn));
        $aTags = array();
        #var_dump($sResetOn);
        //cache tags for mapping later delete
        foreach(explode('|',$sResetOn) as $aTmpTags){
            list($sKey, $sValue) = explode('=', $aTmpTags);
            $aTags[] = strtoupper('OX_DCC_'.$sKey.(empty($sValue) === true ? '': '_'.$sValue));
        }
        $aTags[] = strtoupper('OX_DCC_store_'.$this->getConfig()->getShopId());
        #var_dump($aTags);
        $sId = md5( $sCacheId );
        #var_dump($aTags);
        $sContent = $this->_cleanSensitiveData($sContent);

        $this->oRedis->save($sContent, $sId, $aTags, $this->getCacheLifeTime());
    }

    /**
     * Retrieves a cache entry by it's id.
     *
     * @param string $sCacheId an id for the information to be retrieved
     * @todo addHit
     *
     * @return mixed
     */
    public function get( $sCacheId )
    {
        var_dump($sCacheId);
        #var_dump('GET:'.$sCacheId, md5($sCacheId));
        #var_dump($this->oRedis->getIdsMatchingAnyTags(array('OX_DCC_CID_FC7E7BD8403448F00A363F60F44DA8F2')));
        #$this->resetOn(array('CID'=>'FC7E7BD8403448F00A363F60F44DA8F2'));
        #var_dump($this->oRedis->getIdsMatchingAnyTags(array('OX_DCC_CID_FC7E7BD8403448F00A363F60F44DA8F2')));
        #return false;
        return $this->oRedis->load(md5($sCacheId));
    }

    /**
     * Returns cache id if it exists and is not expired and zend caching is on.
     *
     * @param string $sCacheId caching contents id
     *
     * @return string
     */
    public function getCacheId( $sCacheId )
    {
        var_dump('getCacheId:'.$sCacheId);
        return $this->oRedis->test(md5( $sCacheId ));
    }

    /**
     * Deleted db and file cache content
     *
     * @return mixed
     */
    public function reset()
    {
        $this->oRedis->flushDB();
    }

    /**
     * Resets cache according to special reset conditions passed by params
     *
     * @param array $aResetOn reset conditions array
     * @param bool  $blUseAnd reset precise level ( AND's conditions SQL )
     *
     * @return mixed
     */
    public function resetOn( $aResetOn, $blUseAnd = false )
    {
        $sResetConditions = '';
        $sSep = $blUseAnd ?'and':'or';
        $aTags = array();
        foreach($aResetOn as $sKey => $sValue){
            $aTags[] = strtoupper('OX_DCC_'.$sKey.(empty($sValue) === true ? '': '_'.$sValue));
        }
        var_dump('delete:',$aTags,$sSep);
        switch($sSep){
            case 'and':{
                $this->oRedis->clean(oxcacheredis::CLEANING_MODE_MATCHING_TAG, $aTags);
                break;
            }
            case 'or':{
                $this->oRedis->clean(oxcacheredis::CLEANING_MODE_MATCHING_ANY_TAG, $aTags);
                break;
            }

        }
    }
}
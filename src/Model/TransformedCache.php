<?php

declare(strict_types=1);

namespace Infrangible\Import\Model;

use Magento\Framework\Model\AbstractModel;

/**
 * @author      Andreas Knollmann
 * @copyright   Copyright (c) 2014-2024 Softwareentwicklung Andreas Knollmann
 * @license     http://www.opensource.org/licenses/mit-license.php MIT
 *
 * @method  int getCacheId()
 * @method  void setCacheId(int $cacheId)
 * @method  string getPrefix()
 * @method  void setPrefix(string $prefix)
 * @method  string getHashKey()
 * @method  void setHashKey(string $hashKey)
 * @method  string getDataHash()
 * @method  void setDataHash(string $dataHash)
 * @method  string getCreatedAt()
 * @method  void setCreatedAt(string $createdAt)
 * @method  string getUpdatedAt()
 * @method  void setUpdatedAt(string $updatedAt)
 * @method  string getExpiresAt()
 * @method  void setExpiresAt(string $expiresAt)
 */
class TransformedCache
    extends AbstractModel
{
    /**
     * @return void
     */
    protected function _construct()
    {
        $this->_init(ResourceModel\TransformedCache::class);
    }
}

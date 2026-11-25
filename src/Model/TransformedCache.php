<?php

declare(strict_types=1);

/**
 * @author      Andreas Knollmann
 * @copyright   Copyright (c) 2014-2026 Softwareentwicklung Andreas Knollmann
 * @license     http://www.opensource.org/licenses/mit-license.php MIT
 */

namespace Infrangible\Import\Model;

use Magento\Framework\Exception\LocalizedException;
use Magento\Framework\Model\AbstractModel;

/**
 * An entry of the cache of transformed elements: the hash of the data of an element by prefix and key.
 *
 * @method string getPrefix()
 * @method $this setPrefix(string $prefix)
 * @method string getHashKey()
 * @method $this setHashKey(string $hashKey)
 * @method string getDataHash()
 * @method $this setDataHash(string $dataHash)
 * @method string getExpiresAt()
 * @method $this setExpiresAt(string $expiresAt)
 */
class TransformedCache extends AbstractModel
{
    /**
     * Initializes the resource model.
     *
     * @return void
     * @throws LocalizedException
     */
    protected function _construct(): void
    {
        $this->_init(ResourceModel\TransformedCache::class);
    }
}

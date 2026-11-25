<?php

declare(strict_types=1);

/**
 * @author      Andreas Knollmann
 * @copyright   Copyright (c) 2014-2026 Softwareentwicklung Andreas Knollmann
 * @license     http://www.opensource.org/licenses/mit-license.php MIT
 */

namespace Infrangible\Import\Model\ResourceModel\TransformedCache;

use Infrangible\Import\Model\ResourceModel\TransformedCache as TransformedCacheResource;
use Infrangible\Import\Model\TransformedCache;
use Magento\Framework\Model\ResourceModel\Db\Collection\AbstractCollection;

/**
 * The entries of the cache of transformed elements.
 */
class Collection extends AbstractCollection
{
    /**
     * Initializes the model and the resource model.
     *
     * @return void
     */
    protected function _construct(): void
    {
        $this->_init(TransformedCache::class, TransformedCacheResource::class);
    }
}

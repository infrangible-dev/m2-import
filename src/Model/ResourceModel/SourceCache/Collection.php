<?php

declare(strict_types=1);

/**
 * @author      Andreas Knollmann
 * @copyright   Copyright (c) 2014-2026 Softwareentwicklung Andreas Knollmann
 * @license     http://www.opensource.org/licenses/mit-license.php MIT
 */

namespace Infrangible\Import\Model\ResourceModel\SourceCache;

use Infrangible\Import\Model\ResourceModel\SourceCache as SourceCacheResource;
use Infrangible\Import\Model\SourceCache;
use Magento\Framework\Model\ResourceModel\Db\Collection\AbstractCollection;

/**
 * The entries of the cache of source elements.
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
        $this->_init(SourceCache::class, SourceCacheResource::class);
    }
}

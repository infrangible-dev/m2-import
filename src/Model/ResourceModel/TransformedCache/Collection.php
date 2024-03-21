<?php

declare(strict_types=1);

namespace Infrangible\Import\Model\ResourceModel\TransformedCache;

use Infrangible\Import\Model\TransformedCache;
use Magento\Framework\Model\ResourceModel\Db\Collection\AbstractCollection;

/**
 * @author      Andreas Knollmann
 * @copyright   Copyright (c) 2014-2024 Softwareentwicklung Andreas Knollmann
 * @license     http://www.opensource.org/licenses/mit-license.php MIT
 */
class Collection
    extends AbstractCollection
{
    /**
     * @return void
     */
    protected function _construct()
    {
        $this->_init(TransformedCache::class, \Infrangible\Import\Model\ResourceModel\TransformedCache::class);
    }
}

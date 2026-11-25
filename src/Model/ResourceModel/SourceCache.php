<?php

declare(strict_types=1);

/**
 * @author      Andreas Knollmann
 * @copyright   Copyright (c) 2014-2026 Softwareentwicklung Andreas Knollmann
 * @license     http://www.opensource.org/licenses/mit-license.php MIT
 */

namespace Infrangible\Import\Model\ResourceModel;

use Magento\Framework\Model\ResourceModel\Db\AbstractDb;

/**
 * The table of the cache of source elements.
 */
class SourceCache extends AbstractDb
{
    /**
     * Initializes the table and its ID field.
     *
     * @return void
     */
    protected function _construct(): void
    {
        $this->_init('import_source_cache', 'cache_id');
    }
}

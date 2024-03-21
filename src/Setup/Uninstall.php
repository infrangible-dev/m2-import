<?php

declare(strict_types=1);

namespace Infrangible\Import\Setup;

use Magento\Framework\Setup\ModuleContextInterface;
use Magento\Framework\Setup\SchemaSetupInterface;
use Magento\Framework\Setup\UninstallInterface;

/**
 * @author      Andreas Knollmann
 * @copyright   Copyright (c) 2014-2024 Softwareentwicklung Andreas Knollmann
 * @license     http://www.opensource.org/licenses/mit-license.php MIT
 */
class Uninstall
    implements UninstallInterface
{
    /**
     * Module uninstall code
     *
     * @param SchemaSetupInterface   $setup
     * @param ModuleContextInterface $context
     *
     * @return void
     */
    public function uninstall(
        SchemaSetupInterface $setup,
        ModuleContextInterface $context)
    {
        $setup->startSetup();

        $connection = $setup->getConnection();

        if ($setup->tableExists($setup->getTable('import_source_cache'))) {
            $connection->dropTable($setup->getTable('import_source_cache'));
        }

        if ($setup->tableExists($setup->getTable('import_transformed_cache'))) {
            $connection->dropTable($setup->getTable('import_transformed_cache'));
        }

        $setup->endSetup();
    }
}

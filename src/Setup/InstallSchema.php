<?php

declare(strict_types=1);

namespace Infrangible\Import\Setup;

use Magento\Framework\DB\Adapter\AdapterInterface;
use Magento\Framework\DB\Ddl\Table;
use Magento\Framework\Setup\InstallSchemaInterface;
use Magento\Framework\Setup\ModuleContextInterface;
use Magento\Framework\Setup\SchemaSetupInterface;
use Zend_Db_Exception;

/**
 * @author      Andreas Knollmann
 * @copyright   Copyright (c) 2014-2024 Softwareentwicklung Andreas Knollmann
 * @license     http://www.opensource.org/licenses/mit-license.php MIT
 */
class InstallSchema
    implements InstallSchemaInterface
{
    /**
     * Installs DB schema for a module
     *
     * @param SchemaSetupInterface   $setup
     * @param ModuleContextInterface $context
     *
     * @return void
     * @throws Zend_Db_Exception
     */
    public function install(SchemaSetupInterface $setup, ModuleContextInterface $context)
    {
        $setup->startSetup();

        $connection = $setup->getConnection();

        if ( ! $setup->tableExists($setup->getTable('import_source_cache'))) {
            $sourceCacheTableName = $setup->getTable('import_source_cache');

            $sourceCacheTable = $connection->newTable($sourceCacheTableName);

            $sourceCacheTable->addColumn('cache_id', Table::TYPE_INTEGER, 10, [
                'identity' => true,
                'unsigned' => true,
                'nullable' => false,
                'primary'  => true
            ]);
            $sourceCacheTable->addColumn('prefix', Table::TYPE_TEXT, 255, ['nullable' => false]);
            $sourceCacheTable->addColumn('hash_key', Table::TYPE_TEXT, 255, ['nullable' => false]);
            $sourceCacheTable->addColumn('data_hash', Table::TYPE_TEXT, 255, ['nullable' => false]);
            $sourceCacheTable->addColumn('created_at', Table::TYPE_TIMESTAMP, null, [
                'nullable' => false,
                'default'  => Table::TIMESTAMP_INIT
            ]);
            $sourceCacheTable->addColumn('updated_at', Table::TYPE_TIMESTAMP, null, [
                'nullable' => false,
                'default'  => Table::TIMESTAMP_INIT
            ]);
            $sourceCacheTable->addColumn('expires_at', Table::TYPE_TIMESTAMP, null, [
                'nullable' => false,
                'default'  => Table::TIMESTAMP_INIT
            ]);
            $sourceCacheTable->addIndex('unique_data_hash', ['prefix', 'hash_key'],
                ['type' => AdapterInterface::INDEX_TYPE_UNIQUE]);
            $sourceCacheTable->addIndex('expires_at', ['expires_at'], ['type' => AdapterInterface::INDEX_TYPE_INDEX]);

            $connection->createTable($sourceCacheTable);
        }

        if ( ! $setup->tableExists($setup->getTable('import_transformed_cache'))) {
            $transformedCacheTableName = $setup->getTable('import_transformed_cache');

            $transformedCacheTable = $connection->newTable($transformedCacheTableName);

            $transformedCacheTable->addColumn('cache_id', Table::TYPE_INTEGER, 10, [
                'identity' => true,
                'unsigned' => true,
                'nullable' => false,
                'primary'  => true
            ]);
            $transformedCacheTable->addColumn('prefix', Table::TYPE_TEXT, 255, ['nullable' => false]);
            $transformedCacheTable->addColumn('hash_key', Table::TYPE_TEXT, 255, ['nullable' => false]);
            $transformedCacheTable->addColumn('data_hash', Table::TYPE_TEXT, 255, ['nullable' => false]);
            $transformedCacheTable->addColumn('created_at', Table::TYPE_TIMESTAMP, null, [
                'nullable' => false,
                'default'  => Table::TIMESTAMP_INIT
            ]);
            $transformedCacheTable->addColumn('updated_at', Table::TYPE_TIMESTAMP, null, [
                'nullable' => false,
                'default'  => Table::TIMESTAMP_INIT
            ]);
            $transformedCacheTable->addColumn('expires_at', Table::TYPE_TIMESTAMP, null, [
                'nullable' => false,
                'default'  => Table::TIMESTAMP_INIT
            ]);
            $transformedCacheTable->addIndex('unique_data_hash', ['prefix', 'hash_key'],
                ['type' => AdapterInterface::INDEX_TYPE_UNIQUE]);
            $transformedCacheTable->addIndex('expires_at', ['expires_at'],
                ['type' => AdapterInterface::INDEX_TYPE_INDEX]);

            $connection->createTable($transformedCacheTable);
        }

        $mediaGalleryTable = $setup->getTable('catalog_product_entity_media_gallery');

        if (!$connection->tableColumnExists($mediaGalleryTable, 'hash')) {
            $connection->addColumn($mediaGalleryTable, 'hash', [
                'type'    => Table::TYPE_TEXT,
                'length'  => 32,
                'default' => null,
                'comment' => 'Image blob hash',
                'after'   => 'value'
            ]);
        }

        $setup->endSetup();
    }
}

<?php

declare(strict_types=1);

/**
 * @author      Andreas Knollmann
 * @copyright   Copyright (c) 2014-2026 Softwareentwicklung Andreas Knollmann
 * @license     http://www.opensource.org/licenses/mit-license.php MIT
 */

namespace Infrangible\Import\Helper;

use DateTimeInterface;
use Exception;
use FeWeDev\Base\Strings;
use FeWeDev\Base\Variables;
use Infrangible\Core\Helper\Attribute;
use Infrangible\Core\Helper\Database;
use Infrangible\Core\Helper\EntityType;
use Infrangible\Core\Helper\Stores;
use Infrangible\Import\Model\Related;
use Infrangible\Import\Model\ResourceModel\SourceCache\Collection as SourceCacheCollection;
use Infrangible\Import\Model\ResourceModel\SourceCache\CollectionFactory as SourceCacheCollectionFactory;
use Infrangible\Import\Model\ResourceModel\SourceCacheFactory as SourceCacheResourceFactory;
use Infrangible\Import\Model\ResourceModel\TransformedCache\Collection as TransformedCacheCollection;
use Infrangible\Import\Model\ResourceModel\TransformedCache\CollectionFactory as TransformedCacheCollectionFactory;
use Infrangible\Import\Model\ResourceModel\TransformedCacheFactory as TransformedCacheResourceFactory;
use Infrangible\Import\Model\SourceCache;
use Infrangible\Import\Model\SourceCacheFactory;
use Infrangible\Import\Model\TransformedCache;
use Infrangible\Import\Model\TransformedCacheFactory;
use Magento\Catalog\Helper\Data as CatalogHelper;
use Magento\Catalog\Model\Category;
use Magento\Catalog\Model\Product;
use Magento\Eav\Model\Entity\Attribute as EavAttribute;
use Magento\Eav\Model\Entity\Attribute\ScopedAttributeInterface;
use Magento\Framework\DB\Adapter\AdapterInterface;
use Magento\Framework\Exception\LocalizedException;
use Magento\ImportExport\Model\Import\AbstractEntity;
use Magento\Store\Model\Website;
use Psr\Log\LoggerInterface;
use Stringable;

/**
 * Validates and prepares attribute values for imports and manages the caches of source and transformed elements.
 */
class Data
{
    /** The code of the exception for unknown attributes that are only warned about: the attribute is removed. */
    public const IGNORE_AND_REMOVE_ATTRIBUTE = 99;

    /** Attributes whose scope is global when the prices are global. */
    private const PRICE_ATTRIBUTE_CODES = ['minimal_price', 'price', 'special_price'];

    /** Maximum lengths of the text column types. */
    private const TEXT_LENGTHS = [
        'text'       => AbstractEntity::DB_MAX_TEXT_LENGTH,
        'mediumtext' => 16777215,
        'longtext'   => 4294967295
    ];

    /** @var array<string, array<string, array<string, mixed>>> descriptions of tables by table name */
    private array $tableDescriptions = [];

    /** @var array<string, bool> validation results by entity type, store, attribute and value */
    private array $attributeValueValidations = [];

    /**
     * @param Strings $stringHelper
     * @param Database $databaseHelper
     * @param EntityType $entityTypeHelper
     * @param Stores $storeHelper
     * @param Attribute $attributeHelper
     * @param CatalogHelper $magentoCatalogHelper
     * @param LoggerInterface $logging
     * @param SourceCacheFactory $sourceCacheFactory
     * @param SourceCacheResourceFactory $sourceCacheResourceFactory
     * @param SourceCacheCollectionFactory $sourceCacheCollectionFactory
     * @param TransformedCacheFactory $transformedCacheFactory
     * @param TransformedCacheResourceFactory $transformedCacheResourceFactory
     * @param TransformedCacheCollectionFactory $transformedCacheCollectionFactory
     * @param Variables $variables
     */
    public function __construct(
        protected Strings $stringHelper,
        protected Database $databaseHelper,
        protected EntityType $entityTypeHelper,
        protected Stores $storeHelper,
        protected Attribute $attributeHelper,
        protected CatalogHelper $magentoCatalogHelper,
        protected LoggerInterface $logging,
        protected SourceCacheFactory $sourceCacheFactory,
        protected SourceCacheResourceFactory $sourceCacheResourceFactory,
        protected SourceCacheCollectionFactory $sourceCacheCollectionFactory,
        protected TransformedCacheFactory $transformedCacheFactory,
        protected TransformedCacheResourceFactory $transformedCacheResourceFactory,
        protected TransformedCacheCollectionFactory $transformedCacheCollectionFactory,
        protected Variables $variables
    ) {
    }

    /**
     * Returns the element with its related values as arrays.
     *
     * @param array $element
     * @phpstan-param array<int|string, mixed> $element
     *
     * @return array
     * @phpstan-return array<int|string, mixed>
     */
    public function elementToArray(array $element): array
    {
        return array_map(fn(mixed $value): mixed => $value instanceof Related ? $value->toArray() : $value, $element);
    }

    /**
     * Prepares the value of the attribute for its column.
     *
     * Empty values of date attributes are saved as null, decimals are rounded to 4 digits.
     *
     * @param EavAttribute $attribute
     * @param mixed $attributeValue
     *
     * @return mixed
     * @throws Exception
     */
    public function prepareValueForSave(EavAttribute $attribute, mixed $attributeValue): mixed
    {
        if ($attributeValue === null) {
            return null;
        }

        $type = $attribute->getBackendType();

        if (empty($attributeValue) && in_array($type, ['date', 'datetime', 'timestamp'], true)) {
            return null;
        }

        if ($type === 'decimal' && is_numeric($attributeValue)) {
            return round((float)$attributeValue, 4);
        }

        return $this->prepareColumnValue($this->getBackendTableColumn($attribute), $attributeValue);
    }

    /**
     * Returns the description of the column of the attribute in its backend table.
     *
     * @param EavAttribute $attribute
     *
     * @return array
     * @phpstan-return array<string, mixed>
     * @throws Exception
     */
    public function getBackendTableColumn(EavAttribute $attribute): array
    {
        return $this->getTableColumn(
            $this->variables->stringValueOrDefault($attribute->getBackendTable()),
            $this->variables->stringValueOrDefault($attribute->getAttributeCode())
        );
    }

    /**
     * Returns the description of the column of the table; for tables with a column "value" (EAV tables) this column.
     *
     * @param string $tableName
     * @param string $columnName
     *
     * @return array
     * @phpstan-return array<string, mixed>
     * @throws Exception
     */
    public function getTableColumn(string $tableName, string $columnName): array
    {
        $this->tableDescriptions[ $tableName ] ??=
            $this->databaseHelper->getDefaultConnection()->describeTable($tableName);

        $columns = $this->tableDescriptions[ $tableName ];
        $column = $columns[ 'value' ] ?? $columns[ $columnName ] ?? null;

        if ($column === null) {
            throw new Exception(sprintf('Could not identify column: %s in table: %s', $columnName, $tableName));
        }

        return $column;
    }

    /**
     * Prepares the value for the column.
     *
     * @param array $column the description of the column
     * @phpstan-param array<string, mixed> $column
     * @param mixed $value
     *
     * @return mixed
     */
    public function prepareColumnValue(array $column, mixed $value): mixed
    {
        return $this->databaseHelper->getDefaultConnection()->prepareColumnValue($column, $value);
    }

    /**
     * Validates the value of the attribute, throws an exception if it is not valid.
     *
     * Values of attributes with options that do not exist are added as options, if asked. Unknown attributes throw an
     * exception with the code IGNORE_AND_REMOVE_ATTRIBUTE if they are only warned about. The results are cached by
     * entity type, store, attribute and value.
     *
     * @param AdapterInterface $dbAdapter
     * @param string $entityTypeCode
     * @param string $attributeCode
     * @param mixed $attributeValue
     * @param int $storeId
     * @param bool $addUnknownAttributeOptionValues
     * @param bool $isUnknownAttributesWarnOnly
     * @param array $specialAttributes types of attributes of the entity table by attribute code
     * @phpstan-param array<string, string> $specialAttributes
     * @param bool $test
     *
     * @return void
     * @throws Exception
     */
    public function validateAttribute(
        AdapterInterface $dbAdapter,
        string $entityTypeCode,
        string $attributeCode,
        mixed $attributeValue,
        int $storeId,
        bool $addUnknownAttributeOptionValues = true,
        bool $isUnknownAttributesWarnOnly = false,
        array $specialAttributes = [],
        bool $test = false
    ): void {
        if ($attributeValue !== null && ! is_scalar($attributeValue) && ! $attributeValue instanceof Stringable) {
            throw new Exception(
                sprintf('Attribute value should be scalar for attribute with code: %s', $attributeCode)
            );
        }

        $value = $attributeValue === null ? null : (string)$attributeValue;
        $special = array_key_exists($attributeCode, $specialAttributes);
        $key = implode("\0", [$entityTypeCode, $storeId, $attributeCode, $value ?? "\1"]);

        if (! array_key_exists($key, $this->attributeValueValidations)) {
            if (! $special &&
                ! $this->isKnownAttribute($entityTypeCode, $attributeCode, $isUnknownAttributesWarnOnly)) {

                throw new Exception(
                    sprintf('Unknown attribute: %s', $attributeCode),
                    $isUnknownAttributesWarnOnly ? self::IGNORE_AND_REMOVE_ATTRIBUTE : 0
                );
            }

            $this->attributeValueValidations[ $key ] = $special ?
                $this->isSpecialAttributeValid($specialAttributes[ $attributeCode ], $value) :
                $this->isAttributeValid(
                    $dbAdapter,
                    $entityTypeCode,
                    $attributeCode,
                    $value,
                    $storeId,
                    $addUnknownAttributeOptionValues,
                    $test
                );
        }

        if (! $this->attributeValueValidations[ $key ]) {
            throw new Exception(
                sprintf(
                    $special ? 'Invalid special attribute value: "%s" for attribute: %s' :
                        'Invalid attribute value: "%s" for attribute: %s',
                    $value,
                    $attributeCode
                )
            );
        }
    }

    /**
     * Checks if the entity type has the attribute, logs the reason if not.
     *
     * @param string $entityTypeCode
     * @param string $attributeCode
     * @param bool $isUnknownAttributesWarnOnly
     *
     * @return bool
     */
    private function isKnownAttribute(
        string $entityTypeCode,
        string $attributeCode,
        bool $isUnknownAttributesWarnOnly
    ): bool {
        try {
            $this->attributeHelper->getAttribute($entityTypeCode, $attributeCode);

            return true;
        } catch (Exception $exception) {
            if ($isUnknownAttributesWarnOnly) {
                $this->logging->warning($exception->getMessage());
            } else {
                $this->logging->error($exception);
            }

            return false;
        }
    }

    /**
     * Checks the value of an attribute of the entity table with the type "int" or "string".
     *
     * @param string $type
     * @param string|null $value
     *
     * @return bool
     */
    private function isSpecialAttributeValid(string $type, ?string $value): bool
    {
        return match ($type) {
            'int' => $value !== null && $this->isInteger($value),
            'string' => $value !== null,
            default => false
        };
    }

    /**
     * Checks if the value is a whole number, also with leading zeros or decimal places of zero ("05", "5.0").
     *
     * @param string $value
     *
     * @return bool
     */
    private function isInteger(string $value): bool
    {
        $value = trim($value);

        return is_numeric($value) && (float)$value === floor((float)$value);
    }

    /**
     * Checks the value of an attribute by the type of its column, values of attributes with options by their options.
     *
     * @param AdapterInterface $dbAdapter
     * @param string $entityTypeCode
     * @param string $attributeCode
     * @param string|null $value
     * @param int $storeId
     * @param bool $addUnknownAttributeOptionValues
     * @param bool $test
     *
     * @return bool
     * @throws Exception
     */
    private function isAttributeValid(
        AdapterInterface $dbAdapter,
        string $entityTypeCode,
        string $attributeCode,
        ?string $value,
        int $storeId,
        bool $addUnknownAttributeOptionValues,
        bool $test
    ): bool {
        $attribute = $this->attributeHelper->getAttribute($entityTypeCode, $attributeCode);

        if ($attribute->isStatic()) {
            $attributeType = $this->getStaticAttributeType($dbAdapter, $attribute);
        } else {
            $column = $this->getBackendTableColumn($attribute);

            if ($value === null && ! empty($column[ 'NULLABLE' ])) {
                return true;
            }

            try {
                $attributeType = $this->attributeHelper->getAttributeType($entityTypeCode, $attributeCode);
            } catch (Exception $exception) {
                $this->logging->error($exception);

                return false;
            }
        }

        if ($attributeType === 'select' || $attributeType === 'multiselect') {
            $values = $attributeType === 'multiselect' ? array_map('trim', explode(',', (string)$value)) :
                [(string)$value];

            foreach ($values as $optionValue) {
                if (! $this->isOptionValid(
                    $dbAdapter,
                    $entityTypeCode,
                    $attributeCode,
                    $optionValue,
                    $storeId,
                    $addUnknownAttributeOptionValues,
                    $test
                )) {
                    return false;
                }
            }

            return true;
        }

        if ($value === null) {
            return true;
        }

        $trimmedValue = trim($value);

        return match ($attributeType) {
            'varchar' => $this->stringHelper->strlen($this->stringHelper->cleanString($value)) <
                AbstractEntity::DB_MAX_VARCHAR_LENGTH,
            'decimal' => is_numeric($trimmedValue),
            'tinyint', 'smallint', 'mediumint', 'int', 'bigint' => $this->isInteger($trimmedValue),
            'datetime' => strtotime($trimmedValue) !== false ||
                preg_match('/^\d{2}.\d{2}.\d{2,4}(?:\s+\d{1,2}.\d{1,2}(?:.\d{1,2})?)?$/', $trimmedValue) === 1,
            'text', 'mediumtext', 'longtext' => $this->stringHelper->strlen($this->stringHelper->cleanString($value)) <
                self::TEXT_LENGTHS[ $attributeType ],
            default => true
        };
    }

    /**
     * Returns the type of the column of a static attribute in the entity table.
     *
     * @param AdapterInterface $dbAdapter
     * @param EavAttribute $attribute
     *
     * @return string
     * @throws Exception
     */
    private function getStaticAttributeType(AdapterInterface $dbAdapter, EavAttribute $attribute): string
    {
        $attributeCode = $attribute->getAttributeCode();
        $description = $dbAdapter->describeTable($attribute->getEntity()->getEntityTable());

        foreach ($description as $column) {
            if (($column[ 'COLUMN_NAME' ] ?? null) === $attributeCode) {
                return strtolower($this->variables->stringValueOrDefault($column[ 'DATA_TYPE' ] ?? null));
            }
        }

        throw new Exception(sprintf('Could not identify type for attribute with code: %s', $attributeCode));
    }

    /**
     * Checks if the attribute has an option with the value as label, ID or key, adds it if asked.
     *
     * @param AdapterInterface $dbAdapter
     * @param string $entityTypeCode
     * @param string $attributeCode
     * @param string $value
     * @param int $storeId
     * @param bool $addUnknownAttributeOptionValues
     * @param bool $test
     *
     * @return bool
     * @throws LocalizedException
     */
    private function isOptionValid(
        AdapterInterface $dbAdapter,
        string $entityTypeCode,
        string $attributeCode,
        string $value,
        int $storeId,
        bool $addUnknownAttributeOptionValues,
        bool $test
    ): bool {
        if ($value === '') {
            return false;
        }

        if ($this->attributeHelper->getAttributeOptionId($entityTypeCode, $attributeCode, $storeId, $value) !== null ||
            (preg_match('/^\d+$/', $value) === 1 &&
                $this->attributeHelper->checkAttributeOptionId(
                    $entityTypeCode,
                    $attributeCode,
                    $storeId,
                    $this->variables->intValue($value)
                )) ||
            $this->attributeHelper->checkAttributeOptionKey($entityTypeCode, $attributeCode, $storeId, $value)) {

            return true;
        }

        if (! $addUnknownAttributeOptionValues) {
            return false;
        }

        $this->logging->debug(
            sprintf(
                'Creating value: %s of attribute: %s in store with id: %d as result of unsuccessful validation',
                $value,
                $attributeCode,
                $storeId
            )
        );

        try {
            return $this->attributeHelper->addAttributeOption(
                $dbAdapter,
                $entityTypeCode,
                $attributeCode,
                0,
                $storeId,
                $value,
                $test
            ) !== null;
        } catch (Exception $exception) {
            $this->logging->error($exception);

            return false;
        }
    }

    /**
     * Returns the updates of the value of the attribute of the entity.
     *
     * Attributes of the entity table and static attributes are single updates of the entity table. Product and
     * category attributes update the values of the store (or of all stores of the website for attributes with website
     * scope) and of the admin store, depending on the scope of the attribute; the admin value is the default value of
     * the attribute, if there is one, empty or the value. Prices are global if the catalog prices are global.
     *
     * @param string $entityTypeCode
     * @param string[] $ignoreAttributes
     * @param array $specialAttributes types of attributes of the entity table by attribute code
     * @phpstan-param array<string, string> $specialAttributes
     * @param array $defaultAdminEavAttributeValues
     * @phpstan-param array<string, mixed> $defaultAdminEavAttributeValues
     * @param int $entityId
     * @param int $storeId
     * @param string $attributeCode
     * @param mixed $attributeValue
     * @param bool $addStoreValue
     * @param bool $addAdminValue
     * @param bool $addEmptyAdminValue
     *
     * @return array
     * @phpstan-return list<array{type: string, table: string|null, data: array<string, mixed>}>
     * @throws Exception
     */
    public function createAttributeUpdates(
        string $entityTypeCode,
        array $ignoreAttributes,
        array $specialAttributes,
        array $defaultAdminEavAttributeValues,
        int $entityId,
        int $storeId,
        string $attributeCode,
        mixed $attributeValue,
        bool $addStoreValue,
        bool $addAdminValue,
        bool $addEmptyAdminValue
    ): array {
        $this->logging->debug(
            sprintf(
                'Adding attribute update with entity type code: %s, attribute code: %s in store: %d of entity with ' .
                'id: %d using value: %s, add store value: %s, add admin value: %s and empty admin value: %s',
                $entityTypeCode,
                $attributeCode,
                $storeId,
                $entityId,
                $this->variables->stringValueOrDefault($attributeValue),
                $addStoreValue ? 'yes' : 'no',
                $addAdminValue ? 'yes' : 'no',
                $addEmptyAdminValue ? 'yes' : 'no'
            )
        );

        if (in_array($attributeCode, $ignoreAttributes, true)) {
            return [];
        }

        if (array_key_exists($attributeCode, $specialAttributes)) {
            return [
                [
                    'type'  => 'single',
                    'table' => $this->entityTypeHelper->getEntityTypeTableByEntityTypeCode($entityTypeCode),
                    'data'  => [
                        'entity_id'    => $entityId,
                        $attributeCode => $this->prepareSpecialAttributeValueForSave(
                            $specialAttributes,
                            $attributeCode,
                            $attributeValue
                        )
                    ]
                ]
            ];
        }

        $attribute = $this->attributeHelper->getAttribute($entityTypeCode, $attributeCode);
        $table = $attribute->getBackend()->getTable();
        $value = $this->prepareValueForSave($attribute, $attributeValue);
        $attributeId = $this->variables->intValue($attribute->getId());

        if ($attribute->isStatic()) {
            return [
                ['type' => 'single', 'table' => $table, 'data' => ['entity_id' => $entityId, $attributeCode => $value]]
            ];
        }

        if ($entityTypeCode !== Product::ENTITY && $entityTypeCode !== Category::ENTITY) {
            return [
                [
                    'type'  => 'eav',
                    'table' => $table,
                    'data'  => ['entity_id' => $entityId, 'attribute_id' => $attributeId, 'value' => $value]
                ]
            ];
        }

        $scope = $this->getScope($entityTypeCode, $attribute);
        $updates = [];

        if ($storeId !== 0 && $scope !== ScopedAttributeInterface::SCOPE_GLOBAL && $addStoreValue) {
            $website = $scope === ScopedAttributeInterface::SCOPE_WEBSITE ?
                $this->storeHelper->getStore($storeId)->getWebsite() : null;
            $storeIds = $website instanceof Website ? $website->getStoreIds() : [$storeId];

            $this->logging->debug(sprintf('Store ids: %s', implode(', ', $storeIds)));

            foreach ($storeIds as $updateStoreId) {
                $updates[] = [
                    'type'  => 'eav',
                    'table' => $table,
                    'data'  => [
                        'entity_id'    => $entityId,
                        'attribute_id' => $attributeId,
                        'store_id'     => $this->variables->intValue($updateStoreId),
                        'value'        => $value
                    ]
                ];
            }
        }

        if ($storeId === 0 || $addAdminValue || $scope === ScopedAttributeInterface::SCOPE_GLOBAL) {
            $adminValue = $scope === ScopedAttributeInterface::SCOPE_GLOBAL ? $value :
                (array_key_exists($attributeCode, $defaultAdminEavAttributeValues) ?
                    $defaultAdminEavAttributeValues[ $attributeCode ] : ($addEmptyAdminValue ? null : $value));

            $updates[] = [
                'type'  => 'eav',
                'table' => $table,
                'data'  => [
                    'entity_id'    => $entityId,
                    'attribute_id' => $attributeId,
                    'store_id'     => 0,
                    'value'        => $adminValue
                ]
            ];
        }

        return $updates;
    }

    /**
     * Returns the scope of a product or category attribute; prices are global if the catalog prices are global.
     *
     * @param string $entityTypeCode
     * @param EavAttribute $attribute
     *
     * @return int
     * @throws Exception
     */
    private function getScope(string $entityTypeCode, EavAttribute $attribute): int
    {
        if (in_array($attribute->getAttributeCode(), self::PRICE_ATTRIBUTE_CODES, true) &&
            $this->magentoCatalogHelper->isPriceGlobal()) {

            return ScopedAttributeInterface::SCOPE_GLOBAL;
        }

        $catalogAttribute = $entityTypeCode === Category::ENTITY ?
            $this->attributeHelper->getCatalogCategoryAttribute($attribute) :
            $this->attributeHelper->getCatalogProductAttribute($attribute);

        return $this->variables->intValue($catalogAttribute->getIsGlobal());
    }

    /**
     * Prepares the value of an attribute of the entity table for its type ("int", "string", "datetime" or "date").
     *
     * Values of other types and of other attributes are returned unchanged.
     *
     * @param array $specialAttributes types of attributes of the entity table by attribute code
     * @phpstan-param array<string, string> $specialAttributes
     * @param string $attributeCode
     * @param mixed $attributeValue
     *
     * @return mixed
     */
    public function prepareSpecialAttributeValueForSave(
        array $specialAttributes,
        string $attributeCode,
        mixed $attributeValue
    ): mixed {
        $isDate = $attributeValue instanceof DateTimeInterface;

        return match ($specialAttributes[ $attributeCode ] ?? null) {
            'int' => $this->variables->intValue($attributeValue),
            'string' => $this->variables->stringValueOrDefault($attributeValue),
            'datetime' => $isDate ? $attributeValue->format('Y-m-d H:i:s') :
                $this->variables->stringValueOrDefault($attributeValue),
            'date' => $isDate ? $attributeValue->format('Y-m-d') :
                $this->variables->stringValueOrDefault($attributeValue),
            default => $attributeValue
        };
    }

    /**
     * Returns the ID of the product entity type.
     *
     * @return int|null
     * @throws Exception
     */
    public function getProductEntityTypeId(): ?int
    {
        return $this->entityTypeHelper->getProductEntityTypeId();
    }

    /**
     * Deletes the entry of the source cache with the prefix and key.
     *
     * @param string $sourceCachePrefix
     * @param string $sourceElementHashKeyChunk
     *
     * @return bool true if an entry was deleted
     */
    public function clearCacheSource(string $sourceCachePrefix, string $sourceElementHashKeyChunk): bool
    {
        return $this->clearCache('import_source_cache', $sourceCachePrefix, $sourceElementHashKeyChunk);
    }

    /**
     * Deletes the entry of the transformed cache with the prefix and key.
     *
     * @param string $transformedCachePrefix
     * @param string $transformedElementHashKeyChunk
     *
     * @return bool true if an entry was deleted
     */
    public function clearCacheTransformed(string $transformedCachePrefix, string $transformedElementHashKeyChunk): bool
    {
        return $this->clearCache('import_transformed_cache', $transformedCachePrefix, $transformedElementHashKeyChunk);
    }

    /**
     * Deletes the entry of the cache table with the prefix and key.
     *
     * @param string $tableName
     * @param string $cachePrefix
     * @param string $elementHashKey
     *
     * @return bool true if an entry was deleted
     */
    protected function clearCache(string $tableName, string $cachePrefix, string $elementHashKey): bool
    {
        return $this->databaseHelper->getDefaultConnection()->delete(
            $this->databaseHelper->getTableName($tableName),
            ['prefix = ?' => $cachePrefix, 'hash_key = ?' => $elementHashKey]
        ) > 0;
    }

    /**
     * Returns a new entry of the source cache.
     *
     * @return SourceCache
     */
    public function newSourceCache(): SourceCache
    {
        return $this->sourceCacheFactory->create();
    }

    /**
     * Loads the entry of the source cache.
     *
     * @param int $sourceCacheId
     *
     * @return SourceCache
     */
    public function loadSourceCache(int $sourceCacheId): SourceCache
    {
        $sourceCache = $this->newSourceCache();

        $this->sourceCacheResourceFactory->create()->load($sourceCache, $sourceCacheId);

        return $sourceCache;
    }

    /**
     * Saves the entry of the source cache.
     *
     * @param SourceCache $sourceCache
     *
     * @return void
     * @throws Exception
     */
    public function saveSourceCache(SourceCache $sourceCache): void
    {
        $this->sourceCacheResourceFactory->create()->save($sourceCache);
    }

    /**
     * Returns the entries of the source cache.
     *
     * @return SourceCacheCollection
     */
    public function getSourceCacheCollection(): SourceCacheCollection
    {
        return $this->sourceCacheCollectionFactory->create();
    }

    /**
     * Returns a new entry of the transformed cache.
     *
     * @return TransformedCache
     */
    public function newTransformedCache(): TransformedCache
    {
        return $this->transformedCacheFactory->create();
    }

    /**
     * Loads the entry of the transformed cache.
     *
     * @param int $transformedCacheId
     *
     * @return TransformedCache
     */
    public function loadTransformedCache(int $transformedCacheId): TransformedCache
    {
        $transformedCache = $this->newTransformedCache();

        $this->transformedCacheResourceFactory->create()->load($transformedCache, $transformedCacheId);

        return $transformedCache;
    }

    /**
     * Saves the entry of the transformed cache.
     *
     * @param TransformedCache $transformedCache
     *
     * @return void
     * @throws Exception
     */
    public function saveTransformedCache(TransformedCache $transformedCache): void
    {
        $this->transformedCacheResourceFactory->create()->save($transformedCache);
    }

    /**
     * Returns the entries of the transformed cache.
     *
     * @return TransformedCacheCollection
     */
    public function getTransformedCacheCollection(): TransformedCacheCollection
    {
        return $this->transformedCacheCollectionFactory->create();
    }
}

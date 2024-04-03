<?php

declare(strict_types=1);

namespace Infrangible\Import\Helper;

use Exception;
use FeWeDev\Base\Strings;
use Infrangible\Core\Helper\Database;
use Infrangible\Core\Helper\EntityType;
use Infrangible\Core\Helper\Stores;
use Infrangible\Import\Model\Related;
use Infrangible\Import\Model\ResourceModel\SourceCache\Collection;
use Infrangible\Import\Model\ResourceModel\SourceCache\CollectionFactory;
use Infrangible\Import\Model\SourceCache;
use Infrangible\Import\Model\SourceCacheFactory;
use Infrangible\Import\Model\TransformedCache;
use Infrangible\Import\Model\TransformedCacheFactory;
use Magento\Catalog\Model\Category;
use Magento\Catalog\Model\Product;
use Magento\Eav\Model\Entity\Attribute;
use Magento\Framework\DB\Adapter\AdapterInterface;
use Magento\Framework\Exception\AlreadyExistsException;
use Magento\Framework\Exception\LocalizedException;
use Magento\ImportExport\Model\Import\AbstractEntity;
use Psr\Log\LoggerInterface;
use Zend_Db_Statement_Exception;

/**
 * @author      Andreas Knollmann
 * @copyright   Copyright (c) 2014-2024 Softwareentwicklung Andreas Knollmann
 * @license     http://www.opensource.org/licenses/mit-license.php MIT
 */
class Data
{
    /** @var int */
    public const IGNORE_AND_REMOVE_ATTRIBUTE = 99;

    /** @var array */
    private static $priceAttributeCodes = [
        'minimal_price',
        'price',
        'special_price'
    ];

    /** @var Strings */
    protected $stringHelper;

    /** @var Database */
    protected $databaseHelper;

    /** @var EntityType */
    protected $entityTypeHelper;

    /** @var Stores */
    protected $storeHelper;

    /** @var \Infrangible\Core\Helper\Attribute */
    protected $attributeHelper;

    /** @var \Magento\Catalog\Helper\Data */
    protected $magentoCatalogHelper;

    /** @var LoggerInterface */
    protected $logging;

    /** @var SourceCacheFactory */
    protected $sourceCacheFactory;

    /** @var \Infrangible\Import\Model\ResourceModel\SourceCacheFactory */
    protected $sourceCacheResourceFactory;

    /** @var CollectionFactory */
    protected $sourceCacheCollectionFactory;

    /** @var TransformedCacheFactory */
    protected $transformedCacheFactory;

    /** @var \Infrangible\Import\Model\ResourceModel\TransformedCacheFactory */
    protected $transformedCacheResourceFactory;

    /** @var \Infrangible\Import\Model\ResourceModel\TransformedCache\CollectionFactory */
    protected $transformedCacheCollectionFactory;

    /** @var array */
    private $attributeBackendTables = [];

    /** @var array */
    private $attributeValueValidations = [];

    /**
     * @param Strings                                                                    $stringHelper
     * @param Database                                                                   $databaseHelper
     * @param EntityType                                                                 $entityTypeHelper
     * @param Stores                                                                     $storeHelper
     * @param \Infrangible\Core\Helper\Attribute                                         $attributeHelper
     * @param \Magento\Catalog\Helper\Data                                               $magentoCatalogHelper
     * @param LoggerInterface                                                            $logging
     * @param SourceCacheFactory                                                         $sourceCacheFactory
     * @param \Infrangible\Import\Model\ResourceModel\SourceCacheFactory                 $sourceCacheResourceFactory
     * @param CollectionFactory                                                          $sourceCacheCollectionFactory
     * @param TransformedCacheFactory                                                    $transformedCacheFactory
     * @param \Infrangible\Import\Model\ResourceModel\TransformedCacheFactory            $transformedCacheResourceFactory
     * @param \Infrangible\Import\Model\ResourceModel\TransformedCache\CollectionFactory $transformedCacheCollectionFactory
     */
    public function __construct(
        Strings $stringHelper,
        Database $databaseHelper,
        EntityType $entityTypeHelper,
        Stores $storeHelper,
        \Infrangible\Core\Helper\Attribute $attributeHelper,
        \Magento\Catalog\Helper\Data $magentoCatalogHelper,
        LoggerInterface $logging,
        SourceCacheFactory $sourceCacheFactory,
        \Infrangible\Import\Model\ResourceModel\SourceCacheFactory $sourceCacheResourceFactory,
        CollectionFactory $sourceCacheCollectionFactory,
        TransformedCacheFactory $transformedCacheFactory,
        \Infrangible\Import\Model\ResourceModel\TransformedCacheFactory $transformedCacheResourceFactory,
        \Infrangible\Import\Model\ResourceModel\TransformedCache\CollectionFactory $transformedCacheCollectionFactory
    ) {
        $this->stringHelper = $stringHelper;
        $this->databaseHelper = $databaseHelper;
        $this->entityTypeHelper = $entityTypeHelper;
        $this->storeHelper = $storeHelper;
        $this->attributeHelper = $attributeHelper;
        $this->magentoCatalogHelper = $magentoCatalogHelper;

        $this->logging = $logging;
        $this->sourceCacheFactory = $sourceCacheFactory;
        $this->sourceCacheResourceFactory = $sourceCacheResourceFactory;
        $this->sourceCacheCollectionFactory = $sourceCacheCollectionFactory;
        $this->transformedCacheFactory = $transformedCacheFactory;
        $this->transformedCacheResourceFactory = $transformedCacheResourceFactory;
        $this->transformedCacheCollectionFactory = $transformedCacheCollectionFactory;
    }

    /**
     * @param array $element
     *
     * @return array
     */
    public function elementToArray(array $element): array
    {
        $outputElement = [];

        foreach ($element as $attributeCode => $attributeValue) {
            $outputElement[$attributeCode] =
                $attributeValue instanceof Related ? $attributeValue->toArray() : $attributeValue;
        }

        return $outputElement;
    }

    /**
     * Prepare value for save
     *
     * @param Attribute $attribute
     * @param mixed     $attributeValue
     *
     * @return mixed
     * @throws Exception
     */
    public function prepareValueForSave(Attribute $attribute, $attributeValue)
    {
        if ($attributeValue === null) {
            return null;
        }

        $type = $attribute->getBackendType();

        if (empty($attributeValue) && ($type == 'date' || $type == 'datetime' || $type == 'timestamp')) {
            $attributeValue = null;
        }

        if ($type == 'decimal') {
            return round($attributeValue, 4);
        }

        return $this->prepareColumnValue($this->getBackendTableColumn($attribute), $attributeValue);
    }

    /**
     * @param Attribute $attribute
     *
     * @return array
     * @throws Exception
     */
    public function getBackendTableColumn(Attribute $attribute): array
    {
        return $this->getTableColumn($attribute->getBackendTable(), $attribute->getAttributeCode());
    }

    /**
     * @param string $tableName
     * @param string $columnName
     *
     * @return array
     * @throws Exception
     */
    public function getTableColumn(string $tableName, string $columnName): array
    {
        if (!isset($this->attributeBackendTables[$tableName])) {
            $this->attributeBackendTables[$tableName] =
                $this->databaseHelper->getDefaultConnection()->describeTable($tableName);
        }

        if (isset($this->attributeBackendTables[$tableName]['value'])) {
            return $this->attributeBackendTables[$tableName]['value'];
        } elseif (isset($this->attributeBackendTables[$tableName][$columnName])) {
            return $this->attributeBackendTables[$tableName][$columnName];
        }

        throw new Exception(sprintf('Could not identify column: %s in table: %s', $columnName, $tableName));
    }

    /**
     * @param array $column the column describe array
     * @param mixed $value
     *
     * @return  mixed
     */
    public function prepareColumnValue(array $column, $value)
    {
        return $this->databaseHelper->getDefaultConnection()->prepareColumnValue($column, $value);
    }

    /**
     * @param AdapterInterface $dbAdapter
     * @param string           $entityTypeCode
     * @param string           $attributeCode
     * @param mixed            $attributeValue
     * @param int              $storeId
     * @param bool             $addUnknownAttributeOptionValues
     * @param bool             $isUnknownAttributesWarnOnly
     * @param array            $specialAttributes
     * @param bool             $test
     *
     * @throws Exception
     */
    public function validateAttribute(
        AdapterInterface $dbAdapter,
        string $entityTypeCode,
        string $attributeCode,
        $attributeValue,
        int $storeId,
        bool $addUnknownAttributeOptionValues = true,
        bool $isUnknownAttributesWarnOnly = false,
        array $specialAttributes = [],
        bool $test = false
    ) {
        if (!is_scalar($attributeValue) && !is_null($attributeValue)
            && !(is_object($attributeValue) && method_exists($attributeValue, '__toString'))) {
            throw new Exception(
                sprintf(
                    'Attribute value should be scalar for attribute with code: %s',
                    $attributeCode
                )
            );
        }

        $attributeValueHash = md5($attributeCode.$attributeValue);

        if (array_key_exists($attributeValueHash, $this->attributeValueValidations)) {
            $attributeValidationResult = $this->attributeValueValidations[$attributeValueHash];

            if (!$attributeValidationResult) {
                throw new Exception(
                    sprintf(
                        'Invalid attribute value: "%s" for attribute: %s',
                        $attributeValue,
                        $attributeCode
                    )
                );
            }
        } else {
            if (array_key_exists($attributeCode, $specialAttributes)) {
                $type = $specialAttributes[$attributeCode];

                switch ($type) {
                    case 'int':
                        $attributeValidationResult =
                            is_int($attributeValue) || (((string) (int) $attributeValue) == $attributeValue);
                        break;
                    case 'string':
                        $attributeValidationResult = is_string($attributeValue) || is_numeric($attributeValue);
                        break;
                    default:
                        $attributeValidationResult = false;
                }

                $this->attributeValueValidations[$attributeValueHash] = $attributeValidationResult;

                if (!$attributeValidationResult) {
                    throw new Exception(
                        sprintf(
                            'Invalid special attribute value: "%s" for attribute: %s',
                            $attributeValue,
                            $attributeCode
                        )
                    );
                }
            } else {
                try {
                    $attribute = $this->attributeHelper->getAttribute($entityTypeCode, $attributeCode);
                } catch (Exception $exception) {
                    if ($isUnknownAttributesWarnOnly) {
                        $this->logging->warning($exception->getMessage());
                    } else {
                        $this->logging->error($exception);
                    }

                    $attribute = null;
                }

                if (is_null($attribute)) {
                    throw new Exception(
                        sprintf('Unknown attribute: %s', $attributeCode),
                        $isUnknownAttributesWarnOnly ? static::IGNORE_AND_REMOVE_ATTRIBUTE : 0
                    );
                } else {
                    $attributeValidationResult = $this->isAttributeValid(
                        $dbAdapter,
                        $entityTypeCode,
                        $attributeCode,
                        $attributeValue,
                        $storeId,
                        $addUnknownAttributeOptionValues,
                        $test
                    );

                    $this->attributeValueValidations[$attributeValueHash] = $attributeValidationResult;

                    if (!$attributeValidationResult) {
                        throw new Exception(
                            sprintf(
                                'Invalid attribute value: "%s" for attribute: %s',
                                $attributeValue,
                                $attributeCode
                            )
                        );
                    }
                }
            }
        }
    }

    /**
     * @param AdapterInterface $dbAdapter
     * @param string           $entityTypeCode
     * @param string           $attributeCode
     * @param mixed            $attributeValue
     * @param int              $storeId
     * @param bool             $addUnknownAttributeOptionValues
     * @param bool             $test
     *
     * @return bool
     * @throws Exception
     */
    private function isAttributeValid(
        AdapterInterface $dbAdapter,
        string $entityTypeCode,
        string $attributeCode,
        $attributeValue,
        int $storeId,
        bool $addUnknownAttributeOptionValues,
        bool $test = false
    ): bool {
        $attribute = $this->attributeHelper->getAttribute($entityTypeCode, $attributeCode);

        if ($attribute->isStatic()) {
            $attributeType = null;

            $entityTableName = $attribute->getEntity()->getEntityTable();

            $describe = $dbAdapter->describeTable($entityTableName);

            foreach ($describe as $column) {
                if ($column['COLUMN_NAME'] == $attributeCode) {
                    $attributeType = strtolower($column['DATA_TYPE']);
                }
            }

            if ($attributeType === null) {
                throw new Exception(sprintf('Could not identify type for attribute with code: %s', $attributeCode));
            }
        } else {
            $column = $this->getBackendTableColumn($attribute);

            if (is_null($attributeValue) && array_key_exists('NULLABLE', $column) && $column['NULLABLE']) {
                return true;
            }

            try {
                $attributeType = $this->attributeHelper->getAttributeType($entityTypeCode, $attributeCode);
            } catch (Exception $exception) {
                $this->logging->error($exception);

                return false;
            }
        }

        switch ($attributeType) {
            case 'varchar':
                if (is_array($attributeValue) || is_object($attributeValue)) {
                    $valid = false;
                } elseif ($attributeValue === null) {
                    $valid = true;
                } else {
                    $value = $this->stringHelper->cleanString(strval($attributeValue));
                    $valid = $this->stringHelper->strlen($value) < AbstractEntity::DB_MAX_VARCHAR_LENGTH;
                }
                break;
            case 'decimal':
                $value = trim($attributeValue);
                $valid = (float) $value == $value;
                break;
            case 'select':
                $valid = $this->attributeHelper->getAttributeOptionId(
                        $entityTypeCode,
                        $attributeCode,
                        $storeId,
                        $attributeValue
                    ) !== null
                    || (is_numeric($attributeValue)
                        && $this->attributeHelper->checkAttributeOptionId(
                            $entityTypeCode,
                            $attributeCode,
                            $storeId,
                            $attributeValue
                        ))
                    || ($this->attributeHelper->checkAttributeOptionKey(
                        $entityTypeCode,
                        $attributeCode,
                        $storeId,
                        $attributeValue
                    ));
                if (!$valid && $addUnknownAttributeOptionValues === true) {
                    try {
                        $this->logging->debug(
                            sprintf(
                                'Creating value: %s of attribute: %s in store with id: %d as result of unsuccessful validation',
                                $attributeValue,
                                $attribute->getAttributeCode(),
                                $storeId
                            )
                        );
                        $valid = $this->attributeHelper->addAttributeOption(
                                $dbAdapter,
                                $entityTypeCode,
                                $attributeCode,
                                0,
                                $storeId,
                                $attributeValue,
                                $test
                            ) !== null;
                    } catch (Exception $exception) {
                        $this->logging->error($exception);
                        $valid = false;
                    }
                }
                break;
            case 'multiselect':
                $attributeValues = explode(',', $attributeValue);
                $valid = true;
                foreach ($attributeValues as $attributeValue) {
                    $attributeValue = trim($attributeValue);
                    $singleOptionValid = $this->attributeHelper->getAttributeOptionId(
                            $entityTypeCode,
                            $attributeCode,
                            $storeId,
                            $attributeValue
                        ) != null
                        || (is_numeric($attributeValue)
                            && $this->attributeHelper->checkAttributeOptionId(
                                $entityTypeCode,
                                $attributeCode,
                                $storeId,
                                $attributeValue
                            ))
                        || ($this->attributeHelper->checkAttributeOptionKey(
                            $entityTypeCode,
                            $attributeCode,
                            $storeId,
                            $attributeValue
                        ));
                    if (!$singleOptionValid && $addUnknownAttributeOptionValues === true) {
                        try {
                            $this->logging->debug(
                                sprintf(
                                    'Creating value: %s of attribute: %s in store with id: %d as result of unsuccessful validation',
                                    $attributeValue,
                                    $attribute->getAttributeCode(),
                                    $storeId
                                )
                            );
                            $singleOptionValid = $this->attributeHelper->addAttributeOption(
                                    $dbAdapter,
                                    $entityTypeCode,
                                    $attributeCode,
                                    0,
                                    $storeId,
                                    $attributeValue,
                                    $test
                                ) !== null;
                        } catch (Exception $exception) {
                            $this->logging->error($exception);
                            $singleOptionValid = false;
                        }
                    }
                    $valid = $valid && $singleOptionValid;
                }
                break;
            case 'tinyint':
            case 'smallint':
            case 'mediumint':
            case 'int':
            case 'bigint':
                if ($attributeValue === null) {
                    $valid = true;
                } else {
                    $value = is_string($attributeValue) ? trim($attributeValue) : $attributeValue;
                    $valid = (int) $value == $value;
                }
                break;
            case 'datetime':
                if ($attributeValue === null) {
                    $valid = true;
                } else {
                    $value = is_string($attributeValue) ? trim($attributeValue) : $attributeValue;
                    $valid = strtotime($value)
                        || preg_match('/^\d{2}.\d{2}.\d{2,4}(?:\s+\d{1,2}.\d{1,2}(?:.\d{1,2})?)?$/', $value);
                }
                break;
            case 'text':
                if ($attributeValue === null) {
                    $valid = true;
                } else {
                    $value = $this->stringHelper->cleanString($attributeValue);
                    $valid = $this->stringHelper->strlen($value) < AbstractEntity::DB_MAX_TEXT_LENGTH;
                }
                break;
            case 'mediumtext':
                if ($attributeValue === null) {
                    $valid = true;
                } else {
                    $value = $this->stringHelper->cleanString($attributeValue);
                    $valid = $this->stringHelper->strlen($value) < 16777215;
                }
                break;
            case 'longtext':
                if ($attributeValue === null) {
                    $valid = true;
                } else {
                    $value = $this->stringHelper->cleanString($attributeValue);
                    $valid = $this->stringHelper->strlen($value) < 4294967295;
                }
                break;
            default:
                $valid = true;
                break;
        }

        return $valid;
    }

    /**
     * @param string $entityTypeCode
     * @param array  $ignoreAttributes
     * @param array  $specialAttributes
     * @param array  $defaultAdminEavAttributeValues
     * @param int    $entityId
     * @param int    $storeId
     * @param string $attributeCode
     * @param mixed  $attributeValue
     * @param bool   $addStoreValue
     * @param bool   $addAdminValue
     * @param bool   $addEmptyAdminValue
     *
     * @return array
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
        $attributeValue,
        bool $addStoreValue,
        bool $addAdminValue,
        bool $addEmptyAdminValue
    ): array {
        $this->logging->debug(
            sprintf(
                'Adding attribute update with entity type code: %s, attribute code: %s in store: %d of entity with id: %d using value: %s, add store value: %s, add admin value: %s and empty admin value: %s',
                $entityTypeCode,
                $attributeCode,
                $storeId,
                $entityId,
                $attributeValue,
                $addStoreValue ? 'yes' : 'no',
                $addAdminValue ? 'yes' : 'no',
                $addEmptyAdminValue ? 'yes' : 'no'
            )
        );

        $result = [];

        if (in_array($attributeCode, $ignoreAttributes)) {
            return $result;
        } else {
            if (array_key_exists($attributeCode, $specialAttributes)) {
                $result[] = [
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
                ];
            } else {
                $attribute = $this->attributeHelper->getAttribute($entityTypeCode, $attributeCode);

                $table = $attribute->getBackend()->getTable();

                $preparedAttributeValue = $this->prepareValueForSave($attribute, $attributeValue);

                if ($attribute->isStatic()) {
                    $result[] = [
                        'type'  => 'single',
                        'table' => $table,
                        'data'  => [
                            'entity_id'    => $entityId,
                            $attributeCode => $preparedAttributeValue
                        ]
                    ];
                } else {
                    if ($entityTypeCode == Product::ENTITY || $entityTypeCode == Category::ENTITY) {
                        $catalogAttribute = $entityTypeCode == Category::ENTITY ?
                            $this->attributeHelper->getCatalogCategoryAttribute($attribute) :
                            $this->attributeHelper->getCatalogProductAttribute($attribute);
                        $scope = $catalogAttribute->getIsGlobal();

                        if (in_array($attributeCode, static::$priceAttributeCodes)) {
                            if ($this->magentoCatalogHelper->isPriceGlobal()) {
                                $scope = Attribute\ScopedAttributeInterface::SCOPE_GLOBAL;
                            }
                        }

                        $this->logging->debug(
                            sprintf(
                                'Attribute scope: %s',
                                $scope === Attribute\ScopedAttributeInterface::SCOPE_GLOBAL ? 'global' :
                                    ($scope === Attribute\ScopedAttributeInterface::SCOPE_WEBSITE ? 'website' : 'store')
                            )
                        );

                        $updateStoreValues =
                            $storeId != 0 && $scope !== Attribute\ScopedAttributeInterface::SCOPE_GLOBAL
                            && $addStoreValue;
                        $updateAdminValue = $storeId == 0 || $addAdminValue
                            || $scope === Attribute\ScopedAttributeInterface::SCOPE_GLOBAL;
                        $adminValue =
                            $scope === Attribute\ScopedAttributeInterface::SCOPE_GLOBAL ? $preparedAttributeValue :
                                (array_key_exists($attributeCode, $defaultAdminEavAttributeValues) ?
                                    $defaultAdminEavAttributeValues[$attributeCode] :
                                    ($addEmptyAdminValue ? null : $preparedAttributeValue));

                        $this->logging->debug(sprintf('Update store values: %s', $updateStoreValues ? 'yes' : 'no'));

                        if ($updateStoreValues) {
                            $storeIds = $scope === Attribute\ScopedAttributeInterface::SCOPE_WEBSITE ?
                                $this->storeHelper->getStore($storeId)->getWebsite()->getStoreIds() : [$storeId];

                            $this->logging->debug(sprintf('Store ids: %s', implode(', ', $storeIds)));

                            foreach ($storeIds as $updateStoreId) {
                                $result[] = [
                                    'type'  => 'eav',
                                    'table' => $table,
                                    'data'  => [
                                        'entity_id'    => $entityId,
                                        'attribute_id' => (int) $attribute->getId(),
                                        'store_id'     => (int) $updateStoreId,
                                        'value'        => $preparedAttributeValue
                                    ]
                                ];
                            }
                        }

                        $this->logging->debug(sprintf('Update admin value: %s', $updateAdminValue ? 'yes' : 'no'));

                        if ($updateAdminValue) {
                            $this->logging->debug(sprintf('Admin value: %s', $adminValue));

                            $result[] = [
                                'type'  => 'eav',
                                'table' => $table,
                                'data'  => [
                                    'entity_id'    => $entityId,
                                    'attribute_id' => (int) $attribute->getId(),
                                    'store_id'     => 0,
                                    'value'        => $adminValue
                                ]
                            ];
                        }
                    } else {
                        $result[] = [
                            'type'  => 'eav',
                            'table' => $table,
                            'data'  => [
                                'entity_id'    => $entityId,
                                'attribute_id' => (int) $attribute->getId(),
                                'value'        => $preparedAttributeValue
                            ]
                        ];
                    }
                }
            }
        }

        return $result;
    }

    /**
     * @param array  $specialAttributes
     * @param string $attributeCode
     * @param mixed  $attributeValue
     *
     * @return mixed
     */
    public function prepareSpecialAttributeValueForSave(
        array $specialAttributes,
        string $attributeCode,
        $attributeValue
    ) {
        $result = $attributeValue;

        if (array_key_exists($attributeCode, $specialAttributes)) {
            $type = $specialAttributes[$attributeCode];

            switch ($type) {
                case 'int':
                    $result = (int) $attributeValue;
                    break;
                case 'string':
                    $result = (string) $attributeValue;
                    break;
                case 'datetime':
                    $result = $attributeValue instanceof \DateTime ? $attributeValue->format('Y-m-d H:i:s') :
                        (string) $attributeValue;
                    break;
                case 'date':
                    $result = $attributeValue instanceof \DateTime ? $attributeValue->format('Y-m-d') :
                        (string) $attributeValue;
                    break;
                default:
                    var_dump($attributeValue);
            }
        }

        return $result;
    }

    /**
     * @return int|null
     * @throws LocalizedException
     */
    public function getProductEntityTypeId(): ?int
    {
        $productEntityType = $this->entityTypeHelper->getProductEntityType();

        return empty($productEntityType) ? null : $productEntityType->getId();
    }

    /**
     * Deletes from the source cache table the cache entry matches with given prefix and key.
     *
     * @param string $sourceCachePrefix
     * @param string $sourceElementHashKeyChunk
     *
     * @return bool
     * @throws Zend_Db_Statement_Exception
     */
    public function clearCacheSource(string $sourceCachePrefix, string $sourceElementHashKeyChunk): bool
    {
        return $this->clearCache('import_source_cache', $sourceCachePrefix, $sourceElementHashKeyChunk);
    }

    /**
     * Deletes from the transformed cache table the cache entry matches with given prefix and key.
     *
     * @param string $transformedCachePrefix
     * @param string $transformedElementHashKeyChunk
     *
     * @return bool
     * @throws Zend_Db_Statement_Exception
     */
    public function clearCacheTransformed(string $transformedCachePrefix, string $transformedElementHashKeyChunk): bool
    {
        return $this->clearCache('import_transformed_cache', $transformedCachePrefix, $transformedElementHashKeyChunk);
    }

    /**
     * Deletes from the table to the given model name the cache entry matches with given prefix and key.
     *
     * @param string $tableName
     * @param string $cachePrefix
     * @param string $elementHashKeyChunk
     *
     * @return bool
     * @throws Zend_Db_Statement_Exception
     */
    protected function clearCache(string $tableName, string $cachePrefix, string $elementHashKeyChunk): bool
    {
        // get a active write connection to the DB
        $connection = $this->databaseHelper->getDefaultConnection();

        $tableName = $this->databaseHelper->getTableName($tableName);

        // build SELECT query to select the correct data to delete
        $query = $connection->select();
        $query = $query->from($tableName);
        $query = $query->where('prefix = ?', $cachePrefix);
        $query = $query->where('hash_key = ?', $elementHashKeyChunk);

        // build the corresponding DELETE query from SELECT query
        $query = $query->deleteFromSelect($tableName);

        // execute query and return status
        return $connection->query($query)->execute();
    }

    /**
     * @return SourceCache
     */
    public function newSourceCache(): SourceCache
    {
        return $this->sourceCacheFactory->create();
    }

    /**
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
     * @param SourceCache $sourceCache
     *
     * @throws AlreadyExistsException
     */
    public function saveSourceCache(SourceCache $sourceCache)
    {
        $this->sourceCacheResourceFactory->create()->save($sourceCache);
    }

    /**
     * @return Collection
     */
    public function getSourceCacheCollection(): Collection
    {
        return $this->sourceCacheCollectionFactory->create();
    }

    /**
     * @return TransformedCache
     */
    public function newTransformedCache(): TransformedCache
    {
        return $this->transformedCacheFactory->create();
    }

    /**
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
     * @param TransformedCache $transformedCache
     *
     * @throws AlreadyExistsException
     */
    public function saveTransformedCache(TransformedCache $transformedCache)
    {
        $this->transformedCacheResourceFactory->create()->save($transformedCache);
    }

    /**
     * @return \Infrangible\Import\Model\ResourceModel\TransformedCache\Collection
     */
    public function getTransformedCacheCollection(): \Infrangible\Import\Model\ResourceModel\TransformedCache\Collection
    {
        return $this->transformedCacheCollectionFactory->create();
    }
}

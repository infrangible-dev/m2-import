<?php

declare(strict_types=1);

/**
 * @author      Andreas Knollmann
 * @copyright   Copyright (c) 2014-2026 Softwareentwicklung Andreas Knollmann
 * @license     http://www.opensource.org/licenses/mit-license.php MIT
 */

namespace Infrangible\Import\Test\Unit\Helper;

use DateTime;
use Exception;
use FeWeDev\Base\Strings;
use FeWeDev\Base\Variables;
use Infrangible\Core\Helper\Attribute;
use Infrangible\Core\Helper\Database;
use Infrangible\Core\Helper\EntityType;
use Infrangible\Core\Helper\Stores;
use Infrangible\Import\Helper\Data;
use Infrangible\Import\Model\ResourceModel\SourceCache\CollectionFactory as SourceCacheCollectionFactory;
use Infrangible\Import\Model\ResourceModel\SourceCacheFactory as SourceCacheResourceFactory;
use Infrangible\Import\Model\ResourceModel\TransformedCache\CollectionFactory as TransformedCacheCollectionFactory;
use Infrangible\Import\Model\ResourceModel\TransformedCacheFactory as TransformedCacheResourceFactory;
use Infrangible\Import\Model\SourceCacheFactory;
use Infrangible\Import\Model\TransformedCacheFactory;
use Magento\Catalog\Helper\Data as CatalogHelper;
use Magento\Catalog\Model\ResourceModel\Eav\Attribute as CatalogAttribute;
use Magento\Eav\Model\Entity\Attribute as EavAttribute;
use Magento\Eav\Model\Entity\Attribute\Backend\AbstractBackend;
use Magento\Eav\Model\Entity\Attribute\ScopedAttributeInterface;
use Magento\Framework\DB\Adapter\AdapterInterface;
use Magento\Store\Model\Store;
use Magento\Store\Model\Website;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;

/**
 * Tests the validation of attribute values and the updates of attribute values of imports.
 */
class DataTest extends TestCase
{
    /** @var array<string, string> attribute types by attribute code */
    private array $attributeTypes = [];

    /** @var array<string, int> scopes of the catalog attributes by attribute code */
    private array $scopes = [];

    /** @var list<string> options of the attribute "color" */
    private array $options = ['red'];

    /** @var list<string> values added as options */
    private array $addedOptions = [];

    /** @var int number of type checks */
    private int $typeChecks = 0;

    /**
     * Returns the helper with attributes of the types and scopes of the test.
     *
     * @param bool $priceGlobal
     *
     * @return Data
     */
    private function createHelper(bool $priceGlobal = false): Data
    {
        $connection = $this->createStub(AdapterInterface::class);
        $connection->method('describeTable')->willReturn(['value' => ['COLUMN_NAME' => 'value', 'NULLABLE' => true]]);
        $connection->method('prepareColumnValue')->willReturnArgument(1);

        $databaseHelper = $this->createStub(Database::class);
        $databaseHelper->method('getDefaultConnection')->willReturn($connection);

        $attributeHelper = $this->createStub(Attribute::class);
        $attributeHelper->method('getAttribute')->willReturnCallback(
            function (string $entityTypeCode, string $attributeCode): EavAttribute {
                if (! isset($this->attributeTypes[ $attributeCode ])) {
                    throw new Exception(sprintf('No attribute %s', $attributeCode));
                }

                return $this->createAttribute($attributeCode);
            }
        );
        $attributeHelper->method('getAttributeType')->willReturnCallback(
            function (string $entityTypeCode, string $attributeCode): string {
                $this->typeChecks++;

                return $this->attributeTypes[ $attributeCode ];
            }
        );
        $attributeHelper->method('getAttributeOptionId')->willReturnCallback(
            fn(string $entityTypeCode, string $attributeCode, int $storeId, string $value): ?int =>
            in_array($value, $this->options, true) ? 1 : null
        );
        $attributeHelper->method('addAttributeOption')->willReturnCallback(
            function (
                AdapterInterface $dbAdapter,
                string $entityTypeCode,
                string $attributeCode,
                int $sortOrder,
                int $storeId,
                ?string $value
            ): int {
                $this->addedOptions[] = (string)$value;

                return 2;
            }
        );
        $attributeHelper->method('getCatalogProductAttribute')->willReturnCallback(
            fn(EavAttribute $attribute): CatalogAttribute => $this->createCatalogAttribute(
                $this->scopes[ $attribute->getAttributeCode() ] ?? ScopedAttributeInterface::SCOPE_STORE
            )
        );

        $website = $this->createStub(Website::class);
        $website->method('getStoreIds')->willReturn([1 => '1', 2 => '2']);

        $store = $this->createStub(Store::class);
        $store->method('getWebsite')->willReturn($website);

        $storeHelper = $this->createStub(Stores::class);
        $storeHelper->method('getStore')->willReturn($store);

        $entityTypeHelper = $this->createStub(EntityType::class);
        $entityTypeHelper->method('getEntityTypeTableByEntityTypeCode')->willReturn('catalog_product_entity');

        $catalogHelper = $this->createStub(CatalogHelper::class);
        $catalogHelper->method('isPriceGlobal')->willReturn($priceGlobal);

        return new Data(
            new Strings(),
            $databaseHelper,
            $entityTypeHelper,
            $storeHelper,
            $attributeHelper,
            $catalogHelper,
            $this->createStub(LoggerInterface::class),
            $this->createStub(SourceCacheFactory::class),
            $this->createStub(SourceCacheResourceFactory::class),
            $this->createStub(SourceCacheCollectionFactory::class),
            $this->createStub(TransformedCacheFactory::class),
            $this->createStub(TransformedCacheResourceFactory::class),
            $this->createStub(TransformedCacheCollectionFactory::class),
            new Variables()
        );
    }

    /**
     * Returns an attribute of the product with the backend type of its type.
     *
     * @param string $attributeCode
     *
     * @return EavAttribute
     */
    private function createAttribute(string $attributeCode): EavAttribute
    {
        $backend = $this->createStub(AbstractBackend::class);
        $backend->method('getTable')->willReturn('catalog_product_entity_varchar');

        $attribute = $this->createStub(EavAttribute::class);
        $attribute->method('getAttributeCode')->willReturn($attributeCode);
        $attribute->method('getId')->willReturn('7');
        $attribute->method('isStatic')->willReturn(false);
        $attribute->method('getBackendType')->willReturn($this->attributeTypes[ $attributeCode ]);
        $attribute->method('getBackendTable')->willReturn('catalog_product_entity_varchar');
        $attribute->method('getBackend')->willReturn($backend);

        return $attribute;
    }

    /**
     * Returns a catalog attribute with the scope.
     *
     * @param int $scope
     *
     * @return CatalogAttribute
     */
    private function createCatalogAttribute(int $scope): CatalogAttribute
    {
        $catalogAttribute = $this->createStub(CatalogAttribute::class);
        $catalogAttribute->method('getIsGlobal')->willReturn((string)$scope);

        return $catalogAttribute;
    }

    /**
     * Returns the validation error of the value, null if it is valid.
     *
     * @param Data $helper
     * @param string $attributeCode
     * @param mixed $value
     * @param array $specialAttributes
     * @phpstan-param array<string, string> $specialAttributes
     * @param bool $addOptions
     * @param int $storeId
     *
     * @return string|null
     */
    private function validate(
        Data $helper,
        string $attributeCode,
        mixed $value,
        array $specialAttributes = [],
        bool $addOptions = false,
        int $storeId = 1
    ): ?string {
        try {
            $helper->validateAttribute(
                $this->createStub(AdapterInterface::class),
                'catalog_product',
                $attributeCode,
                $value,
                $storeId,
                $addOptions,
                true,
                $specialAttributes
            );

            return null;
        } catch (Exception $exception) {
            return $exception->getMessage();
        }
    }

    public function testValidatesSpecialAttributes(): void
    {
        $helper = $this->createHelper();
        $special = ['position' => 'int', 'title' => 'string', 'created' => 'datetime'];

        $this->assertNull($this->validate($helper, 'position', '5', $special));
        $this->assertNull($this->validate($helper, 'position', '05', $special));
        $this->assertNull($this->validate($helper, 'position', 5.0, $special));
        $this->assertSame(
            'Invalid special attribute value: "five" for attribute: position',
            $this->validate($helper, 'position', 'five', $special)
        );
        $this->assertNull($this->validate($helper, 'title', 12, $special));
        $this->assertNotNull($this->validate($helper, 'created', '2026-01-01', $special));
    }

    public function testValidatesValuesByType(): void
    {
        $this->attributeTypes = ['weight' => 'decimal', 'qty' => 'int', 'name' => 'varchar', 'from' => 'datetime'];
        $helper = $this->createHelper();

        $this->assertNull($this->validate($helper, 'weight', '1.5'));
        $this->assertNotNull($this->validate($helper, 'weight', '1,5'));
        $this->assertNull($this->validate($helper, 'qty', ' 3 '));
        $this->assertNull($this->validate($helper, 'qty', 3));
        $this->assertNotNull($this->validate($helper, 'qty', '3.5'));
        $this->assertNull($this->validate($helper, 'name', 'Bag'));
        $this->assertNotNull($this->validate($helper, 'name', str_repeat('x', 300)));
        $this->assertNull($this->validate($helper, 'from', '2026-11-25 10:00:00'));
        $this->assertNull($this->validate($helper, 'from', '25.11.2026'));
        $this->assertNotNull($this->validate($helper, 'from', 'soon'));
        $this->assertNull($this->validate($helper, 'name', null));
        $this->assertSame(
            'Attribute value should be scalar for attribute with code: name',
            $this->validate($helper, 'name', ['Bag'])
        );
    }

    public function testUnknownAttributeIsIgnoredIfOnlyWarnedAbout(): void
    {
        $helper = $this->createHelper();

        try {
            $helper->validateAttribute(
                $this->createStub(AdapterInterface::class),
                'catalog_product',
                'unknown',
                'x',
                1,
                false,
                true
            );

            $this->fail('No exception');
        } catch (Exception $exception) {
            $this->assertSame(Data::IGNORE_AND_REMOVE_ATTRIBUTE, $exception->getCode());
        }
    }

    public function testValidatesOptionsAndAddsUnknownOnesIfAsked(): void
    {
        $this->attributeTypes = ['color' => 'select', 'sizes' => 'multiselect'];
        $helper = $this->createHelper();

        $this->assertNull($this->validate($helper, 'color', 'red'));
        $this->assertNotNull($this->validate($helper, 'color', 'blue'));
        $this->assertNull($this->validate($helper, 'color', 'green', [], true));
        $this->assertNull($this->validate($helper, 'sizes', 'red, yellow', [], true));
        $this->assertNotNull($this->validate($helper, 'color', '', [], true));
        $this->assertSame(['green', 'yellow'], $this->addedOptions);
    }

    public function testValidationResultsAreCachedByStore(): void
    {
        $this->attributeTypes = ['qty' => 'int'];
        $helper = $this->createHelper();

        $this->validate($helper, 'qty', '3');
        $this->validate($helper, 'qty', '3');
        $this->validate($helper, 'qty', '3', [], false, 2);

        $this->assertSame(2, $this->typeChecks);
    }

    /**
     * @throws Exception
     */
    public function testUpdatesOfSpecialAttribute(): void
    {
        $updates = $this->createHelper()->createAttributeUpdates(
            'catalog_product',
            [],
            ['position' => 'int'],
            [],
            10,
            1,
            'position',
            '5',
            true,
            false,
            false
        );

        $this->assertSame(
            [['type' => 'single', 'table' => 'catalog_product_entity', 'data' => ['entity_id' => 10, 'position' => 5]]],
            $updates
        );
    }

    /**
     * @throws Exception
     */
    public function testUpdatesOfWebsiteAttributeInStore(): void
    {
        $this->attributeTypes = ['name' => 'varchar'];
        $this->scopes = ['name' => ScopedAttributeInterface::SCOPE_WEBSITE];

        $updates = $this->createHelper()->createAttributeUpdates(
            'catalog_product',
            [],
            [],
            ['name' => 'Default'],
            10,
            1,
            'name',
            'Bag',
            true,
            true,
            false
        );

        $this->assertSame(
            [[1, 'Bag'], [2, 'Bag'], [0, 'Default']],
            array_map(
                fn(array $update): array => [$update[ 'data' ][ 'store_id' ], $update[ 'data' ][ 'value' ]],
                $updates
            )
        );
    }

    /**
     * @throws Exception
     */
    public function testUpdatesOfGlobalPrice(): void
    {
        $this->attributeTypes = ['price' => 'decimal', 'ignored' => 'varchar'];

        $helper = $this->createHelper(true);

        $updates = $helper->createAttributeUpdates(
            'catalog_product',
            [],
            [],
            [],
            10,
            1,
            'price',
            '9.12345',
            true,
            false,
            false
        );

        $this->assertSame(
            [
                [
                    'type'  => 'eav',
                    'table' => 'catalog_product_entity_varchar',
                    'data'  => ['entity_id' => 10, 'attribute_id' => 7, 'store_id' => 0, 'value' => 9.1235]
                ]
            ],
            $updates
        );
        $this->assertSame(
            [],
            $helper->createAttributeUpdates(
                'catalog_product',
                ['ignored'],
                [],
                [],
                10,
                1,
                'ignored',
                'x',
                true,
                true,
                true
            )
        );
    }

    public function testPrepareSpecialAttributeValueForSave(): void
    {
        $helper = $this->createHelper();
        $types = ['id' => 'int', 'from' => 'datetime', 'day' => 'date', 'other' => 'json'];
        $date = new DateTime('2026-11-25 10:11:12');

        $this->assertSame(5, $helper->prepareSpecialAttributeValueForSave($types, 'id', '5'));
        $this->assertSame('2026-11-25 10:11:12', $helper->prepareSpecialAttributeValueForSave($types, 'from', $date));
        $this->assertSame('2026-11-25', $helper->prepareSpecialAttributeValueForSave($types, 'day', $date));
        $this->assertSame(['x'], $helper->prepareSpecialAttributeValueForSave($types, 'other', ['x']));
        $this->assertSame('y', $helper->prepareSpecialAttributeValueForSave($types, 'unknown', 'y'));
    }
}

<?php

declare(strict_types=1);

/**
 * @author      Andreas Knollmann
 * @copyright   Copyright (c) 2014-2026 Softwareentwicklung Andreas Knollmann
 * @license     http://www.opensource.org/licenses/mit-license.php MIT
 */

namespace Infrangible\Import\Test\Unit\Model;

use DateTime;
use Exception;
use FeWeDev\Base\Arrays;
use FeWeDev\Base\Variables;
use Infrangible\Core\Helper\Database;
use Infrangible\Core\Helper\Instances;
use Infrangible\Import\Helper\Data;
use Infrangible\Import\Model\ResourceModel\SourceCache\Collection as SourceCacheCollection;
use Magento\Framework\DB\Adapter\AdapterInterface;
use Magento\Framework\DB\Select;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;

/**
 * Tests the steps of an import with the cache of source elements.
 */
class ImportTest extends TestCase
{
    /** @var list<array<string, mixed>> entries of the source cache */
    private array $cacheEntries = [];

    /**
     * Returns an import of the source elements that imports every transformed element.
     *
     * @param array $source
     * @phpstan-param array<int, array{sku: string, name: string}> $source
     *
     * @return TestImport
     */
    private function createImport(array $source): TestImport
    {
        $select = $this->createStub(Select::class);
        $select->method('assemble')->willReturn('SELECT');

        $collection = $this->createStub(SourceCacheCollection::class);
        $collection->method('getSelect')->willReturn($select);

        $importHelper = $this->createStub(Data::class);
        $importHelper->method('getSourceCacheCollection')->willReturn($collection);
        $importHelper->method('elementToArray')->willReturnArgument(0);

        $databaseHelper = $this->createStub(Database::class);
        $databaseHelper->method('getDefaultConnection')->willReturn($this->createStub(AdapterInterface::class));
        $databaseHelper->method('fetchAssoc')->willReturnCallback(fn(): array => $this->cacheEntries);

        $import = new TestImport(
            new Arrays(new Variables()),
            new Variables(),
            $databaseHelper,
            $this->createStub(Instances::class),
            $importHelper,
            $this->createStub(LoggerInterface::class)
        );

        $import->source = $source;
        $import->setUseSourceDataCache();
        $import->setSourceCachePrefix('test');
        $import->setTest();

        return $import;
    }

    /**
     * Returns a cache entry of the element.
     *
     * @param string $sku
     * @param array $element
     * @phpstan-param array{sku: string, name: string} $element
     * @param string $expiresAt
     *
     * @return array
     * @phpstan-return array<string, mixed>
     */
    private function createCacheEntry(string $sku, array $element, string $expiresAt): array
    {
        return [
            'cache_id'   => '1',
            'hash_key'   => $sku,
            'data_hash'  => hash('sha256', (string)json_encode($element)),
            'expires_at' => $expiresAt
        ];
    }

    /**
     * @throws Exception
     */
    public function testImportsValidElements(): void
    {
        $import = $this->createImport(
            [1 => ['sku' => 'A', 'name' => 'alpha'], 2 => ['sku' => 'B', 'name' => '']]
        );

        $import->run();

        $this->assertSame(['A'], $import->imported);
        $this->assertSame([2], $import->getSourceInvalidElementNumbers());
        $this->assertSame(['2: name missing'], $import->invalid);
        $this->assertTrue($import->hasErrors());
    }

    /**
     * @throws Exception
     */
    public function testSkipsElementsThatAreCachedUnchanged(): void
    {
        $alpha = ['sku' => 'A', 'name' => 'alpha'];
        $beta = ['sku' => 'B', 'name' => 'beta'];
        $gamma = ['sku' => 'C', 'name' => 'gamma'];
        $future = (new DateTime('+1 hour'))->format('Y-m-d H:i:s');
        $past = (new DateTime('-1 hour'))->format('Y-m-d H:i:s');

        $this->cacheEntries = [
            $this->createCacheEntry('A', $alpha, $future),
            $this->createCacheEntry('B', ['sku' => 'B', 'name' => 'old'], $future),
            $this->createCacheEntry('C', $gamma, $past)
        ];

        $import = $this->createImport([1 => $alpha, 2 => $beta, 3 => $gamma]);

        $import->run();

        $this->assertSame(['B', 'C'], $import->imported);
    }

    /**
     * @throws Exception
     */
    public function testCacheExpiryInSeconds(): void
    {
        $import = $this->createImport([]);

        $import->setSourceCacheExpiry(3600);
        $import->setTransformedCacheExpiry('60');

        $this->assertSame([], $import->imported);
    }

    public function testRemovesUnchangedElementNumbers(): void
    {
        $import = $this->createImport([]);

        $import->markUnchanged([3, 5, 7]);
        $import->unmarkUnchanged(5);

        $this->assertSame([3, 7], $import->getUnchanged());
    }
}

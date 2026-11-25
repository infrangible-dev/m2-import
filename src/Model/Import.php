<?php

declare(strict_types=1);

/**
 * @author      Andreas Knollmann
 * @copyright   Copyright (c) 2014-2026 Softwareentwicklung Andreas Knollmann
 * @license     http://www.opensource.org/licenses/mit-license.php MIT
 */

namespace Infrangible\Import\Model;

use DateInterval;
use DateTime;
use Exception;
use FeWeDev\Base\Arrays;
use FeWeDev\Base\Variables;
use Infrangible\Core\Helper\Database;
use Infrangible\Core\Helper\Instances;
use Infrangible\Import\Helper\Data;
use Magento\Framework\DB\Adapter\AdapterInterface;
use Magento\Framework\Model\AbstractModel;
use Magento\Framework\Model\ResourceModel\Db\Collection\AbstractCollection;
use Psr\Log\LoggerInterface;

/**
 * The steps of an import: reading, validating, preparing and transforming the source elements, validating and
 * importing the transformed elements.
 *
 * Source and transformed elements can be cached: elements whose hash did not change since the last import (and whose
 * cache entry has not expired) are skipped. The cache entries are saved after the import, for source elements only
 * if all transformed elements of them were imported or cached.
 */
abstract class Import
{
    /** The key of the reason in the reasons of invalid elements. */
    public const REASON = 'reason';

    /** The key of the severity in the reasons of invalid elements. */
    public const SEVERITY = 'severity';

    /** The format of the expiry dates of the cache entries. */
    private const DATE_FORMAT = 'Y-m-d H:i:s';

    /** @var array<int|string, mixed> */
    private array $sourceData = [];

    /** @var array<int, int|string> */
    private array $sourceCachedElementNumbers = [];

    /** @var array<int, int|string> */
    private array $sourceInvalidElementNumbers = [];

    /** @var array<int|string, list<array<string, string>>> */
    private array $sourceInvalidElementReasons = [];

    /** @var array<int|string, mixed> */
    private array $transformedData = [];

    /** @var array<int, int|string> */
    private array $transformedCachedElementNumbers = [];

    /** @var array<int, int|string> */
    private array $transformedInvalidElementNumbers = [];

    /** @var array<int, int> */
    private array $transformedChangedElementNumbers = [];

    /** @var array<int, int> */
    private array $transformedUnchangedElementNumbers = [];

    /** @var bool */
    private bool $useSourceDataCache = false;

    /** @var array<int|string, string> */
    private array $sourceElementHashKeys = [];

    /** @var array<int|string, string> */
    private array $sourceElementHashValues = [];

    /** @var string */
    private string $sourceCachePrefix = '';

    /** @var array<int|string, int> */
    private array $sourceCacheIds = [];

    /** @var bool */
    private bool $useTransformedDataCache = false;

    /** @var array<int|string, string> */
    private array $transformedElementHashKeys = [];

    /** @var array<int|string, string> */
    private array $transformedElementHashValues = [];

    /** @var string */
    private string $transformedCachePrefix = '';

    /** @var array<int|string, int> */
    private array $transformedCacheIds = [];

    /** @var array<int|string, list<array<string, string>>> */
    private array $transformedInvalidElementReasons = [];

    /** @var DateTime|null */
    private ?DateTime $sourceCacheExpiry = null;

    /** @var list<array{source: int, transformed: int}> */
    private array $sourceTransformedRelations = [];

    /** @var array<int, int> */
    private array $transformedImportedElementNumbers = [];

    /** @var DateTime|null */
    private ?DateTime $transformedCacheExpiry = null;

    /** @var bool */
    private bool $test = false;

    /** @var array<string, string> */
    private array $associatedItemModels = [];

    /**
     * @param Arrays $arrays
     * @param Variables $variables
     * @param Database $databaseHelper
     * @param Instances $instanceHelper
     * @param Data $importHelper
     * @param LoggerInterface $logging
     */
    public function __construct(
        protected Arrays $arrays,
        protected Variables $variables,
        protected Database $databaseHelper,
        protected Instances $instanceHelper,
        protected Data $importHelper,
        protected LoggerInterface $logging
    ) {
    }

    /**
     * Runs the import.
     *
     * @return void
     * @throws Exception
     */
    public function run(): void
    {
        $this->prepare();
        $this->runImport();
        $this->cleanup();
    }

    /**
     * Prepares the import.
     *
     * @return void
     */
    abstract protected function prepare(): void;

    /**
     * Reads, validates and transforms the source elements, then validates and imports the transformed elements.
     *
     * @return void
     * @throws Exception
     */
    protected function runImport(): void
    {
        $this->sourceData = $this->readSourceData();

        if (count($this->sourceData) === 0) {
            $this->logging->info('Found no source elements');

            return;
        }

        $this->logging->info(sprintf('Read %d source element(s)', count($this->sourceData)));

        $this->sourceInvalidElementNumbers = [];

        $this->validateSourceData($this->sourceData);

        $this->checkCachedSourceData();

        $unCachedSourceData = $this->withoutElementNumbers($this->sourceData, $this->sourceCachedElementNumbers);

        if (count($unCachedSourceData) === 0) {
            $this->logging->info(sprintf('No %s to transform', $this->getEntitiesLogName()));

            return;
        }

        $remainingSourceData = $this->withoutElementNumbers($unCachedSourceData, $this->sourceInvalidElementNumbers);

        if (count($remainingSourceData) === 0) {
            $this->displaySourceInvalidElements();

            $this->logging->info(sprintf('No %s to transform', $this->getEntitiesLogName()));

            return;
        }

        $preparedSourceData = $this->prepareSourceData($remainingSourceData);

        $this->sourceTransformedRelations = [];

        $this->transformedData = $this->transformData($preparedSourceData);

        $this->displaySourceInvalidElements();

        $this->logging->info(sprintf('Transformed %d element(s)', count($this->transformedData)));

        $dbAdapter = $this->databaseHelper->getDefaultConnection();

        $dbAdapter->beginTransaction();

        $this->transformedInvalidElementNumbers = [];

        $this->validateTransformedData($dbAdapter);

        $dbAdapter->commit();

        $this->checkCachedTransformedData();

        $unCachedTransformedData =
            $this->withoutElementNumbers($this->transformedData, $this->transformedCachedElementNumbers);

        if (count($unCachedTransformedData) === 0) {
            $this->logging->info(sprintf('No %s to validate', $this->getEntitiesLogName()));

            $this->saveSourceCache();

            return;
        }

        $remainingTransformedData =
            $this->withoutElementNumbers($unCachedTransformedData, $this->transformedInvalidElementNumbers);

        $this->displayTransformedInvalidElements();

        if (count($remainingTransformedData) === 0) {
            $this->saveSourceCache();
            $this->saveTransformedCache();

            $this->logging->info(sprintf('No %s to import', $this->getEntitiesLogName()));

            return;
        }

        $this->transformedImportedElementNumbers = [];
        $this->transformedChangedElementNumbers = [];
        $this->transformedUnchangedElementNumbers = [];

        $this->importTransformedData($dbAdapter);

        $this->logging->info(
            sprintf(
                'Imported %d changed %s',
                count($this->transformedChangedElementNumbers),
                $this->getEntitiesLogName()
            )
        );
        $this->logging->info(
            sprintf(
                'Ignored %d unchanged %s',
                count($this->transformedUnchangedElementNumbers),
                $this->getEntitiesLogName()
            )
        );

        $this->saveSourceCache();
        $this->saveTransformedCache();
    }

    /**
     * Returns the elements without those with the numbers.
     *
     * @param array $elements
     * @phpstan-param array<int|string, mixed> $elements
     * @param array $elementNumbers
     * @phpstan-param array<int|string, int|string> $elementNumbers
     *
     * @return array
     * @phpstan-return array<int|string, mixed>
     */
    private function withoutElementNumbers(array $elements, array $elementNumbers): array
    {
        return array_filter(
            $elements,
            fn(int|string $elementNumber): bool => ! in_array($elementNumber, $elementNumbers),
            ARRAY_FILTER_USE_KEY
        );
    }

    /**
     * Imports the valid transformed elements.
     *
     * @param AdapterInterface $dbAdapter
     *
     * @return void
     */
    abstract protected function importTransformedData(AdapterInterface $dbAdapter): void;

    /**
     * Returns the numbers of the transformed elements that were changed by the import.
     *
     * @return array
     * @phpstan-return array<int, int>
     */
    public function getTransformedChangedElementNumbers(): array
    {
        return $this->transformedChangedElementNumbers;
    }

    /**
     * Returns the source elements by element number.
     *
     * @return array
     * @phpstan-return array<int|string, mixed>
     */
    abstract protected function readSourceData(): array;

    /**
     * Validates the source elements and marks invalid ones with addSourceInvalidElementReason().
     *
     * @param array $sourceData
     * @phpstan-param array<int|string, mixed> $sourceData
     *
     * @return void
     */
    abstract protected function validateSourceData(array $sourceData): void;

    /**
     * Finds the valid source elements that are cached unchanged.
     *
     * @return void
     * @throws Exception
     */
    protected function checkCachedSourceData(): void
    {
        $this->sourceCacheIds = [];
        $this->sourceCachedElementNumbers = [];

        if (! $this->isUseSourceDataCache()) {
            return;
        }

        foreach ($this->sourceData as $elementNumber => $element) {
            if (in_array($elementNumber, $this->sourceInvalidElementNumbers)) {
                continue;
            }

            $elementHashKey = $this->getSourceElementHashKey(is_array($element) ? $element : [$element]);
            $elementHashValue = $this->getSourceElementHashValue($element);

            if ($elementHashKey !== '' && $elementHashValue !== '') {
                $this->sourceElementHashKeys[ $elementNumber ] = $elementHashKey;
                $this->sourceElementHashValues[ $elementNumber ] = $elementHashValue;
            }
        }

        [$this->sourceCachedElementNumbers, $this->sourceCacheIds] = $this->findCachedElements(
            fn(): AbstractCollection => $this->importHelper->getSourceCacheCollection(),
            $this->getSourceCachePrefix(),
            $this->sourceElementHashKeys,
            $this->sourceElementHashValues,
            2500
        );

        $this->logging->info(sprintf('Found %d cached source element(s)', count($this->sourceCachedElementNumbers)));
    }

    /**
     * Finds the cache entries of the elements by their hash keys.
     *
     * Returns the numbers of the elements whose hash is cached unchanged and not expired, and the IDs of the cache
     * entries of all found elements.
     *
     * @param callable $createCollection returns a new collection of the cache entries
     * @phpstan-param callable(): AbstractCollection $createCollection
     * @param string $prefix
     * @param array $hashKeys
     * @phpstan-param array<int|string, string> $hashKeys
     * @param array $hashValues
     * @phpstan-param array<int|string, string> $hashValues
     * @param int $chunkSize
     *
     * @return array
     * @phpstan-return array{0: list<int|string>, 1: array<int|string, int>}
     * @throws Exception
     */
    private function findCachedElements(
        callable $createCollection,
        string $prefix,
        array $hashKeys,
        array $hashValues,
        int $chunkSize
    ): array {
        $cachedElementNumbers = [];
        $cacheIds = [];
        $now = new DateTime();

        foreach (array_chunk($hashKeys, max(1, $chunkSize), true) as $hashKeyChunk) {
            $elementNumbers = array_flip($hashKeyChunk);

            $collection = $createCollection();
            $collection->addFieldToFilter('prefix', ['eq' => $prefix]);
            $collection->addFieldToFilter('hash_key', ['in' => array_values($hashKeyChunk)]);

            $this->logging->debug(sprintf('Executing query: %s', $collection->getSelect()->assemble()));

            foreach ($this->databaseHelper->fetchAssoc($collection->getSelect()) as $cacheEntry) {
                $hashKey = $this->variables->stringValueOrDefault($cacheEntry[ 'hash_key' ] ?? null);
                $elementNumber = $elementNumbers[ $hashKey ] ?? null;

                if ($elementNumber === null) {
                    continue;
                }

                $expiresAt = DateTime::createFromFormat(
                    self::DATE_FORMAT,
                    $this->variables->stringValueOrDefault($cacheEntry[ 'expires_at' ] ?? null)
                );

                if (($cacheEntry[ 'data_hash' ] ?? null) === ($hashValues[ $elementNumber ] ?? null) &&
                    $expiresAt !== false && $now < $expiresAt) {

                    $cachedElementNumbers[] = $elementNumber;
                }

                $cacheIds[ $elementNumber ] = $this->variables->intValue($cacheEntry[ 'cache_id' ] ?? 0);
            }
        }

        return [$cachedElementNumbers, $cacheIds];
    }

    /**
     * Returns the name of the imported entities for the log, for example "products".
     *
     * @return string
     */
    abstract protected function getEntitiesLogName(): string;

    /**
     * Logs the invalid source elements with their reasons.
     *
     * @return void
     */
    private function displaySourceInvalidElements(): void
    {
        $this->logging->info(sprintf('Found %d invalid source element(s)', count($this->sourceInvalidElementNumbers)));

        foreach ($this->sourceInvalidElementNumbers as $elementNumber) {
            $this->logging->debug(sprintf('Found invalid source element: %s', $elementNumber));

            foreach ($this->sourceInvalidElementReasons[ $elementNumber ] ?? [] as $elementReason) {
                $this->displaySourceInvalidElement(
                    $this->variables->intValue($elementNumber),
                    $this->sourceData[ $elementNumber ] ?? null,
                    $elementReason[ self::REASON ] ?? '',
                    $elementReason[ self::SEVERITY ] ?? 'error'
                );
            }
        }
    }

    /**
     * Prepares the valid source elements that are not cached for the transformation.
     *
     * @param array $sourceData
     * @phpstan-param array<int|string, mixed> $sourceData
     *
     * @return array
     * @phpstan-return array<int|string, mixed>
     */
    abstract protected function prepareSourceData(array $sourceData): array;

    /**
     * Transforms the source elements into the elements to import, by element number.
     *
     * @param array $sourceData
     * @phpstan-param array<int|string, mixed> $sourceData
     *
     * @return array
     * @phpstan-return array<int|string, mixed>
     */
    abstract protected function transformData(array $sourceData): array;

    /**
     * Validates the transformed elements and marks invalid ones with addTransformedInvalidElementReason().
     *
     * @param AdapterInterface $dbAdapter
     *
     * @return bool
     */
    abstract protected function validateTransformedData(AdapterInterface $dbAdapter): bool;

    /**
     * Finds the valid transformed elements that are cached unchanged.
     *
     * @return void
     * @throws Exception
     */
    protected function checkCachedTransformedData(): void
    {
        $this->transformedCacheIds = [];
        $this->transformedCachedElementNumbers = [];

        if (! $this->isUseTransformedDataCache()) {
            return;
        }

        foreach ($this->transformedData as $elementNumber => $element) {
            if (in_array($elementNumber, $this->transformedInvalidElementNumbers) || ! is_array($element)) {
                continue;
            }

            $elementArray = $this->importHelper->elementToArray($element);

            $elementHashKey = $this->getTransformedElementHashKey($elementArray);
            $elementHashValue = $this->getTransformedElementHashValue($elementArray);

            if ($elementHashKey !== '' && $elementHashValue !== '') {
                $this->transformedElementHashKeys[ $elementNumber ] = $elementHashKey;
                $this->transformedElementHashValues[ $elementNumber ] = $elementHashValue;
            }
        }

        [$this->transformedCachedElementNumbers, $this->transformedCacheIds] = $this->findCachedElements(
            fn(): AbstractCollection => $this->importHelper->getTransformedCacheCollection(),
            $this->transformedCachePrefix,
            $this->transformedElementHashKeys,
            $this->transformedElementHashValues,
            1000
        );

        $this->logging->info(
            sprintf('Found %d cached transformed element(s)', count($this->transformedCachedElementNumbers))
        );
    }

    /**
     * Logs the invalid transformed elements with their reasons.
     *
     * @return void
     */
    private function displayTransformedInvalidElements(): void
    {
        $this->logging->info(
            sprintf('Found %d invalid transformed element(s)', count($this->transformedInvalidElementNumbers))
        );

        foreach ($this->transformedInvalidElementNumbers as $elementNumber) {
            $this->logging->debug(sprintf('Found invalid transformed element: %s', $elementNumber));

            foreach ($this->transformedInvalidElementReasons[ $elementNumber ] ?? [] as $elementReason) {
                $this->displayTransformedInvalidElement(
                    $this->variables->intValue($elementNumber),
                    $this->transformedData[ $elementNumber ] ?? null,
                    $elementReason[ self::REASON ] ?? '',
                    $elementReason[ self::SEVERITY ] ?? 'error'
                );
            }
        }
    }

    /**
     * Saves the hashes of the source elements whose transformed elements were all imported or cached.
     *
     * @return void
     */
    private function saveSourceCache(): void
    {
        if (! $this->isUseSourceDataCache()) {
            return;
        }

        $transformedElementNumbersBySource = [];

        foreach ($this->sourceTransformedRelations as $relation) {
            $transformedElementNumbersBySource[ $relation[ 'source' ] ][] = $relation[ 'transformed' ];
        }

        $validSourceElementNumbers = [];
        $done = array_merge($this->transformedImportedElementNumbers, $this->transformedCachedElementNumbers);

        foreach ($transformedElementNumbersBySource as $sourceElementNumber => $transformedElementNumbers) {
            if (count(array_diff($transformedElementNumbers, $done)) === 0 &&
                ! in_array($sourceElementNumber, $this->sourceCachedElementNumbers)) {

                $validSourceElementNumbers[] = $sourceElementNumber;
            }
        }

        $this->saveCacheEntries(
            'source',
            $validSourceElementNumbers,
            $this->sourceCacheIds,
            $this->sourceElementHashKeys,
            $this->sourceElementHashValues,
            $this->sourceCachePrefix,
            $this->sourceCacheExpiry ?? $this->getDefaultExpiry(),
            fn(int $cacheId): AbstractModel => $this->importHelper->loadSourceCache($cacheId),
            fn(): AbstractModel => $this->importHelper->newSourceCache(),
            function (AbstractModel $cacheEntry): void {
                if ($cacheEntry instanceof SourceCache) {
                    $this->importHelper->saveSourceCache($cacheEntry);
                }
            }
        );
    }

    /**
     * Saves the hashes of the imported transformed elements.
     *
     * @return void
     */
    private function saveTransformedCache(): void
    {
        if (! $this->isUseTransformedDataCache()) {
            return;
        }

        $validElementNumbers = array_filter(
            $this->transformedImportedElementNumbers,
            fn(int $elementNumber): bool => array_key_exists($elementNumber, $this->transformedElementHashValues) &&
                ! in_array($elementNumber, $this->transformedCachedElementNumbers)
        );

        $this->saveCacheEntries(
            'transformed',
            $validElementNumbers,
            $this->transformedCacheIds,
            $this->transformedElementHashKeys,
            $this->transformedElementHashValues,
            $this->transformedCachePrefix,
            $this->transformedCacheExpiry ?? $this->getDefaultExpiry(),
            fn(int $cacheId): AbstractModel => $this->importHelper->loadTransformedCache($cacheId),
            fn(): AbstractModel => $this->importHelper->newTransformedCache(),
            function (AbstractModel $cacheEntry): void {
                if ($cacheEntry instanceof TransformedCache) {
                    $this->importHelper->saveTransformedCache($cacheEntry);
                }
            }
        );
    }

    /**
     * Returns the expiry of cache entries without an expiry set: in one year.
     *
     * @return DateTime
     */
    private function getDefaultExpiry(): DateTime
    {
        return (new DateTime())->add(new DateInterval('P1Y'));
    }

    /**
     * Saves the hashes of the elements in the cache; in test mode they are only logged.
     *
     * @param string $type "source" or "transformed", for the log
     * @param array $elementNumbers
     * @phpstan-param array<int|string, int|string> $elementNumbers
     * @param array $cacheIds
     * @phpstan-param array<int|string, int> $cacheIds
     * @param array $hashKeys
     * @phpstan-param array<int|string, string> $hashKeys
     * @param array $hashValues
     * @phpstan-param array<int|string, string> $hashValues
     * @param string $prefix
     * @param DateTime $expiry
     * @param callable $loadEntry
     * @phpstan-param callable(int): AbstractModel $loadEntry
     * @param callable $newEntry
     * @phpstan-param callable(): AbstractModel $newEntry
     * @param callable $saveEntry
     * @phpstan-param callable(AbstractModel): void $saveEntry
     *
     * @return void
     */
    private function saveCacheEntries(
        string $type,
        array $elementNumbers,
        array $cacheIds,
        array $hashKeys,
        array $hashValues,
        string $prefix,
        DateTime $expiry,
        callable $loadEntry,
        callable $newEntry,
        callable $saveEntry
    ): void {
        $this->logging->info(sprintf('Saving %d %s data hash(es)', count($elementNumbers), $type));

        foreach ($elementNumbers as $elementNumber) {
            if (array_key_exists($elementNumber, $cacheIds)) {
                $cacheEntry = $loadEntry($cacheIds[ $elementNumber ]);
            } elseif (array_key_exists($elementNumber, $hashKeys)) {
                $cacheEntry = $newEntry();
                $cacheEntry->setData('prefix', $prefix);
                $cacheEntry->setData('hash_key', $hashKeys[ $elementNumber ]);
            } else {
                $this->logging->warning(
                    sprintf(
                        'Could not save %s data hash because no cache key was found for element with number: %s',
                        $type,
                        $elementNumber
                    )
                );

                continue;
            }

            $cacheEntry->setData('data_hash', $hashValues[ $elementNumber ] ?? '');
            $cacheEntry->setData('expires_at', $expiry->format(self::DATE_FORMAT));

            $description = sprintf(
                '%s data hash with prefix: %s and key: %s which expires at: %s',
                $type,
                $this->variables->stringValueOrDefault($cacheEntry->getData('prefix')),
                $this->variables->stringValueOrDefault($cacheEntry->getData('hash_key')),
                $this->variables->stringValueOrDefault($cacheEntry->getData('expires_at'))
            );

            $this->logging->debug(sprintf('Saving %s', $description));

            if ($this->isTest()) {
                continue;
            }

            try {
                $saveEntry($cacheEntry);
            } catch (Exception $exception) {
                $this->logging->error(
                    sprintf('Could not save %s because: %s', $description, $exception->getMessage())
                );
                $this->logging->error($exception);
            }
        }
    }

    /**
     * Checks if the source elements are cached.
     *
     * @return bool
     */
    protected function isUseSourceDataCache(): bool
    {
        return $this->useSourceDataCache;
    }

    /**
     * Sets if the source elements are cached.
     *
     * @param bool $useSourceDataCache
     *
     * @return void
     */
    public function setUseSourceDataCache(bool $useSourceDataCache = true): void
    {
        $this->useSourceDataCache = $useSourceDataCache;
    }

    /**
     * Returns the key of the source element in the cache.
     *
     * @param array $sourceElement
     * @phpstan-param array<int|string, mixed> $sourceElement
     *
     * @return string
     */
    abstract protected function getSourceElementHashKey(array $sourceElement): string;

    /**
     * Returns the hash of the data of the source element.
     *
     * @param mixed $sourceElement
     *
     * @return string
     */
    protected function getSourceElementHashValue(mixed $sourceElement): string
    {
        return hash('sha256', (string)json_encode($sourceElement));
    }

    /**
     * Returns the prefix of the keys of the source elements in the cache.
     *
     * @return string
     */
    public function getSourceCachePrefix(): string
    {
        return $this->sourceCachePrefix;
    }

    /**
     * Sets the prefix of the keys of the source elements in the cache.
     *
     * @param string $sourceCachePrefix
     *
     * @return void
     */
    public function setSourceCachePrefix(string $sourceCachePrefix): void
    {
        $this->sourceCachePrefix = $sourceCachePrefix;
    }

    /**
     * Shows an invalid source element with the reason.
     *
     * @param int $sourceElementNumber
     * @param mixed $sourceElement
     * @param string $reason
     * @param string $severity
     *
     * @return void
     */
    abstract protected function displaySourceInvalidElement(
        int $sourceElementNumber,
        mixed $sourceElement,
        string $reason,
        string $severity = 'error'
    ): void;

    /**
     * Checks if the transformed elements are cached.
     *
     * @return bool
     */
    protected function isUseTransformedDataCache(): bool
    {
        return $this->useTransformedDataCache;
    }

    /**
     * Sets if the transformed elements are cached.
     *
     * @param bool $useTransformedDataCache
     *
     * @return void
     */
    public function setUseTransformedDataCache(bool $useTransformedDataCache = true): void
    {
        $this->useTransformedDataCache = $useTransformedDataCache;
    }

    /**
     * Returns the key of the transformed element in the cache.
     *
     * @param array $element
     * @phpstan-param array<int|string, mixed> $element
     *
     * @return string
     */
    abstract protected function getTransformedElementHashKey(array $element): string;

    /**
     * Returns the hash of the data of the transformed element.
     *
     * @param array $element
     * @phpstan-param array<int|string, mixed> $element
     *
     * @return string
     */
    protected function getTransformedElementHashValue(array $element): string
    {
        return hash('sha256', (string)json_encode($element));
    }

    /**
     * Shows an invalid transformed element with the reason.
     *
     * @param int $transformedElementNumber
     * @param mixed $transformedElement
     * @param string $reason
     * @param string $severity
     *
     * @return void
     */
    abstract protected function displayTransformedInvalidElement(
        int $transformedElementNumber,
        mixed $transformedElement,
        string $reason,
        string $severity = 'error'
    ): void;

    /**
     * Sets the prefix of the keys of the transformed elements in the cache.
     *
     * @param string $transformedCachePrefix
     *
     * @return void
     */
    public function setTransformedCachePrefix(string $transformedCachePrefix): void
    {
        $this->transformedCachePrefix = $transformedCachePrefix;
    }

    /**
     * Checks if the import only tests: the cache entries are not saved.
     *
     * @return bool
     */
    public function isTest(): bool
    {
        return $this->test;
    }

    /**
     * Sets if the import only tests.
     *
     * @param bool $test
     *
     * @return void
     */
    public function setTest(bool $test = true): void
    {
        $this->test = $test;
    }

    /**
     * Sets the expiry of the saved source cache entries: a date, or the number of seconds from now.
     *
     * @param DateTime|int|string $sourceCacheExpiry
     *
     * @return void
     * @throws Exception
     */
    public function setSourceCacheExpiry(DateTime|int|string $sourceCacheExpiry): void
    {
        $this->sourceCacheExpiry = $this->getExpiry($sourceCacheExpiry);
    }

    /**
     * Sets the expiry of the saved transformed cache entries: a date, or the number of seconds from now.
     *
     * @param DateTime|int|string $transformedCacheExpiry
     *
     * @return void
     * @throws Exception
     */
    public function setTransformedCacheExpiry(DateTime|int|string $transformedCacheExpiry): void
    {
        $this->transformedCacheExpiry = $this->getExpiry($transformedCacheExpiry);
    }

    /**
     * Returns the date, or the date in the number of seconds from now.
     *
     * @param DateTime|int|string $expiry
     *
     * @return DateTime
     * @throws Exception
     */
    private function getExpiry(DateTime|int|string $expiry): DateTime
    {
        if ($expiry instanceof DateTime) {
            return $expiry;
        }

        return (new DateTime())->add(new DateInterval(sprintf('PT%dS', $this->variables->intValue($expiry))));
    }

    /**
     * Marks the transformed element as invalid with the reason.
     *
     * @param int $elementNumber
     * @param string $reason
     * @param string $severity
     *
     * @return void
     */
    protected function addTransformedInvalidElementReason(
        int $elementNumber,
        string $reason,
        string $severity = 'error'
    ): void {
        $this->logging->debug(
            sprintf('Marking transformed element with number: %d as invalid with reason: %s', $elementNumber, $reason)
        );

        if (! in_array($elementNumber, $this->transformedInvalidElementNumbers)) {
            $this->transformedInvalidElementNumbers[] = $elementNumber;
        }

        $this->transformedInvalidElementReasons[ $elementNumber ][] =
            [self::REASON => $reason, self::SEVERITY => $severity];
    }

    /**
     * Returns the numbers of the invalid source elements.
     *
     * @return array
     * @phpstan-return array<int, int|string>
     */
    public function getSourceInvalidElementNumbers(): array
    {
        return $this->sourceInvalidElementNumbers;
    }

    /**
     * Marks the source element as invalid with the reason.
     *
     * @param int $elementNumber
     * @param string $reason
     * @param string $severity
     *
     * @return void
     */
    protected function addSourceInvalidElementReason(
        int $elementNumber,
        string $reason,
        string $severity = 'error'
    ): void {
        $this->logging->debug(
            sprintf('Marking source element with number: %d as invalid with reason: %s', $elementNumber, $reason)
        );

        if (! in_array($elementNumber, $this->sourceInvalidElementNumbers)) {
            $this->sourceInvalidElementNumbers[] = $elementNumber;
        }

        $this->sourceInvalidElementReasons[ $elementNumber ][] =
            [self::REASON => $reason, self::SEVERITY => $severity];
    }

    /**
     * Returns the invalid transformed elements by element number.
     *
     * @return array
     * @phpstan-return array<int|string, mixed>
     */
    public function getTransformedInvalidElementsData(): array
    {
        $result = [];

        foreach ($this->transformedInvalidElementNumbers as $elementNumber) {
            $result[ $elementNumber ] = $this->transformedData[ $elementNumber ] ?? null;
        }

        return $result;
    }

    /**
     * Relates the transformed element to the source element it was transformed from.
     *
     * @param int $sourceElementNumber
     * @param int $transformedElementNumber
     *
     * @return void
     */
    protected function addSourceTransformedRelation(int $sourceElementNumber, int $transformedElementNumber): void
    {
        $this->sourceTransformedRelations[] =
            ['source' => $sourceElementNumber, 'transformed' => $transformedElementNumber];
    }

    /**
     * Checks if the source element is invalid.
     *
     * @param int $elementNumber
     *
     * @return bool
     */
    protected function isSourceInvalidElementNumber(int $elementNumber): bool
    {
        return in_array($elementNumber, $this->sourceInvalidElementNumbers);
    }

    /**
     * Checks if source or transformed elements are invalid.
     *
     * @return bool
     */
    public function hasErrors(): bool
    {
        return count($this->sourceInvalidElementNumbers) > 0 || count($this->transformedInvalidElementNumbers) > 0;
    }

    /**
     * Adds a transformed element, with the number or as next element.
     *
     * @param array $element
     * @phpstan-param array<int|string, mixed> $element
     * @param int|null $elementNumber
     *
     * @return void
     */
    protected function addTransformedData(array $element, ?int $elementNumber = null): void
    {
        if ($elementNumber === null) {
            $this->transformedData[] = $element;
        } else {
            $this->transformedData[ $elementNumber ] = $element;
        }
    }

    /**
     * Marks the transformed element as unchanged by the import.
     *
     * @param int $elementNumber
     *
     * @return void
     */
    protected function addTransformedUnchangedElementNumber(int $elementNumber): void
    {
        $this->transformedUnchangedElementNumbers[] = $elementNumber;
    }

    /**
     * Removes the mark of the transformed element as unchanged.
     *
     * @param int $elementNumber
     *
     * @return void
     */
    protected function removeTransformedUnchangedElementNumbers(int $elementNumber): void
    {
        $this->transformedUnchangedElementNumbers = array_values(
            array_filter(
                $this->transformedUnchangedElementNumbers,
                fn(int $unchangedElementNumber): bool => $unchangedElementNumber !== $elementNumber
            )
        );
    }

    /**
     * Marks the transformed element as changed by the import.
     *
     * @param int $elementNumber
     *
     * @return void
     */
    protected function addTransformedChangedElementNumbers(int $elementNumber): void
    {
        $this->transformedChangedElementNumbers[] = $elementNumber;
    }

    /**
     * Marks the transformed element as imported.
     *
     * @param int $elementNumber
     *
     * @return void
     */
    protected function addTransformedImportedElementNumbers(int $elementNumber): void
    {
        $this->transformedImportedElementNumbers[] = $elementNumber;
    }

    /**
     * Returns the numbers of the invalid transformed elements.
     *
     * @return array
     * @phpstan-return array<int, int|string>
     */
    public function getTransformedInvalidElementNumbers(): array
    {
        return $this->transformedInvalidElementNumbers;
    }

    /**
     * Returns the numbers of the transformed elements that are cached unchanged.
     *
     * @return array
     * @phpstan-return array<int, int|string>
     */
    protected function getTransformedCachedElementNumbers(): array
    {
        return $this->transformedCachedElementNumbers;
    }

    /**
     * Returns the numbers of the imported transformed elements.
     *
     * @return array
     * @phpstan-return array<int, int>
     */
    protected function getTransformedImportedElementNumbers(): array
    {
        return $this->transformedImportedElementNumbers;
    }

    /**
     * Returns the numbers of the transformed elements that were not changed by the import.
     *
     * @return array
     * @phpstan-return array<int, int>
     */
    protected function getTransformedUnchangedElementNumbers(): array
    {
        return $this->transformedUnchangedElementNumbers;
    }

    /**
     * Returns the transformed elements by element number.
     *
     * @return array
     * @phpstan-return array<int|string, mixed>
     */
    public function getTransformedData(): array
    {
        return $this->transformedData;
    }

    /**
     * Returns the classes of the models of associated items by key.
     *
     * @return array
     * @phpstan-return array<string, string>
     */
    public function getAssociatedItemModels(): array
    {
        return $this->associatedItemModels;
    }

    /**
     * Adds the class of the model of associated items with the key.
     *
     * @param string $key
     * @param string $model
     *
     * @return void
     */
    public function addAssociatedItemModel(string $key, string $model): void
    {
        $this->associatedItemModels[ $key ] = $model;
    }

    /**
     * Cleans up after the import.
     *
     * @return void
     */
    abstract protected function cleanup(): void;
}

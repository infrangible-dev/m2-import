<?php

declare(strict_types=1);

namespace Infrangible\Import\Model;

use DateTime;
use Exception;
use FeWeDev\Base\Arrays;
use FeWeDev\Base\Variables;
use Infrangible\Core\Helper\Database;
use Infrangible\Core\Helper\Instances;
use Infrangible\Import\Helper\Data;
use Magento\Framework\DB\Adapter\AdapterInterface;
use Psr\Log\LoggerInterface;

/**
 * @author      Andreas Knollmann
 * @copyright   Copyright (c) 2014-2024 Softwareentwicklung Andreas Knollmann
 * @license     http://www.opensource.org/licenses/mit-license.php MIT
 */
abstract class Import
{
    /** @var string */
    public const REASON = 'reason';

    /** @var string */
    public const SEVERITY = 'severity';

    /** @var LoggerInterface */
    protected $logging;

    /** @var Arrays */
    protected $arrays;

    /** @var Variables */
    protected $variables;

    /** @var Database */
    protected $databaseHelper;

    /** @var Instances */
    protected $instanceHelper;

    /** @var Data */
    protected $importHelper;

    /** @var array */
    private $sourceData = [];

    /** @var array */
    private $sourceCachedElementNumbers = [];

    /** @var array */
    private $sourceInvalidElementNumbers = [];

    /** @var array */
    private $sourceInvalidElementReasons = [];

    /** @var array */
    private $transformedData = [];

    /** @var array */
    private $transformedCachedElementNumbers = [];

    /** @var array */
    private $transformedInvalidElementNumbers = [];

    /** @var array */
    private $transformedChangedElementNumbers = [];

    /** @var array */
    private $transformedUnchangedElementNumbers = [];

    /** @var bool */
    private $useSourceDataCache = false;

    /** @var array */
    private $sourceElementHashKeys = [];

    /** @var array */
    private $sourceElementHashValues = [];

    /** @var string */
    private $sourceCachePrefix;

    /** @var array */
    private $sourceCacheIds = [];

    /** @var bool */
    private $useTransformedDataCache = false;

    /** @var array */
    private $transformedElementHashKeys = [];

    /** @var array */
    private $transformedElementHashValues = [];

    /** @var string */
    private $transformedCachePrefix = '';

    /** @var array */
    private $transformedCacheIds = [];

    /** @var array */
    private $transformedInvalidElementReasons = [];

    /** @var DateTime */
    private $sourceCacheExpiry;

    /** @var array */
    private $sourceTransformedRelations = [];

    /** @var array */
    private $transformedImportedElementNumbers = [];

    /** @var DateTime */
    private $transformedCacheExpiry;

    /** @var bool */
    private $test = false;

    /** @var array */
    private $associatedItemModels = [];

    /**
     * @param Arrays          $arrays
     * @param Variables       $variables
     * @param Database        $databaseHelper
     * @param Instances       $instanceHelper
     * @param Data            $importHelper
     * @param LoggerInterface $logging
     */
    public function __construct(
        Arrays $arrays,
        Variables $variables,
        Database $databaseHelper,
        Instances $instanceHelper,
        Data $importHelper,
        LoggerInterface $logging
    ) {
        $this->arrays = $arrays;
        $this->variables = $variables;
        $this->databaseHelper = $databaseHelper;
        $this->instanceHelper = $instanceHelper;
        $this->importHelper = $importHelper;
        $this->logging = $logging;
    }

    /**
     * @return void
     * @throws Exception
     */
    public function run()
    {
        $this->prepare();
        $this->runImport();
        $this->cleanup();
    }

    /**
     * @return void
     */
    abstract protected function prepare();

    /**
     * @return void
     * @throws Exception
     */
    protected function runImport()
    {
        $this->sourceData = $this->readSourceData();

        if (count($this->sourceData) == 0) {
            $this->logging->info('Found no source elements');

            return;
        }

        $this->logging->info(sprintf('Read %d source element(s)', count($this->sourceData)));

        $this->sourceInvalidElementNumbers = [];

        $this->validateSourceData($this->sourceData);

        $this->checkCachedSourceData();

        $unCachedSourceData = [];

        foreach ($this->sourceData as $elementNumber => $element) {
            if (!in_array($elementNumber, $this->sourceCachedElementNumbers)) {
                $unCachedSourceData[$elementNumber] = $element;
            }
        }

        if (count($unCachedSourceData) == 0) {
            $this->logging->info(sprintf('No %s to transform', $this->getEntitiesLogName()));

            return;
        }

        $remainingSourceData = [];

        foreach ($unCachedSourceData as $elementNumber => $element) {
            if (!in_array($elementNumber, $this->sourceInvalidElementNumbers)) {
                $remainingSourceData[$elementNumber] = $element;
            }
        }

        if (count($remainingSourceData) == 0) {
            $this->displaySourceInvalidElements();

            $this->logging->info(sprintf('No %s to transform', $this->getEntitiesLogName()));

            return;
        }

        $this->prepareSourceData($remainingSourceData);

        $this->sourceTransformedRelations = [];

        $this->transformedData = $this->transformData($remainingSourceData);

        $this->displaySourceInvalidElements();

        $this->logging->info(sprintf('Transformed %d element(s)', count($this->transformedData)));

        $dbAdapter = $this->databaseHelper->getDefaultConnection();

        $dbAdapter->beginTransaction();

        $this->transformedInvalidElementNumbers = [];

        $this->validateTransformedData($dbAdapter);

        $dbAdapter->commit();

        $this->checkCachedTransformedData();

        $unCachedTransformedData = [];

        foreach ($this->transformedData as $elementNumber => $element) {
            if (!in_array($elementNumber, $this->transformedCachedElementNumbers)) {
                $unCachedTransformedData[$elementNumber] = $element;
            }
        }

        if (count($unCachedTransformedData) == 0) {
            $this->logging->info(sprintf('No %s to validate', $this->getEntitiesLogName()));

            $this->saveSourceCache();

            return;
        }

        $hasRemainingTransformedData = false;

        foreach (array_keys($unCachedTransformedData) as $elementNumber) {
            if (!in_array($elementNumber, $this->transformedInvalidElementNumbers)) {
                $hasRemainingTransformedData = true;

                break;
            }
        }

        $this->displayTransformedInvalidElements();

        if ($hasRemainingTransformedData === false) {
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
     * @param AdapterInterface $dbAdapter
     *
     * @return void
     */
    abstract protected function importTransformedData(AdapterInterface $dbAdapter);

    /**
     * @return array
     */
    public function getTransformedChangedElementNumbers(): array
    {
        return $this->transformedChangedElementNumbers;
    }

    /**
     * @return array
     */
    abstract protected function readSourceData(): array;

    /**
     * @param array $sourceData
     *
     * @return void
     */
    abstract protected function validateSourceData(array $sourceData);

    /**
     * @return void
     * @throws Exception
     */
    protected function checkCachedSourceData()
    {
        $this->sourceCacheIds = [];

        if (!$this->isUseSourceDataCache()) {
            return;
        }

        foreach ($this->sourceData as $elementNumber => $element) {
            if (in_array($elementNumber, $this->sourceInvalidElementNumbers)) {
                continue;
            }

            $elementHashKey = $this->getSourceElementHashKey($element);
            $elementHashValue = $this->getSourceElementHashValue($element);

            if (!empty($elementHashKey) && !empty($elementHashValue)) {
                $this->sourceElementHashKeys[$elementNumber] = $elementHashKey;
                $this->sourceElementHashValues[$elementNumber] = $elementHashValue;
            }
        }

        $sourceElementHashKeyChunks = array_chunk($this->sourceElementHashKeys, 2500, true);

        $now = new DateTime();

        foreach ($sourceElementHashKeyChunks as $sourceElementHashKeyChunk) {
            $cacheEntryCollection = $this->importHelper->getSourceCacheCollection();

            $cacheEntryCollection->addFieldToFilter('prefix', ['eq' => $this->getSourceCachePrefix()]);
            $cacheEntryCollection->addFieldToFilter('hash_key', ['in' => $sourceElementHashKeyChunk]);

            $cacheEntries = $this->databaseHelper->fetchAssoc($cacheEntryCollection->getSelect());

            foreach ($cacheEntries as $cacheEntry) {
                foreach ($sourceElementHashKeyChunk as $elementNumber => $elementHashKey) {
                    if (array_key_exists('hash_key', $cacheEntry) && $cacheEntry['hash_key'] === $elementHashKey) {
                        $expiresAt = DateTime::createFromFormat(
                            'Y-m-d H:m:s',
                            $this->arrays->getValue($cacheEntry, 'expires_at')
                        );

                        if (array_key_exists('data_hash', $cacheEntry)
                            && $cacheEntry['data_hash'] === $this->sourceElementHashValues[$elementNumber]
                            && $now->getTimestamp() < $expiresAt->getTimestamp()) {
                            $this->sourceCachedElementNumbers[] = $elementNumber;
                        }

                        $this->sourceCacheIds[$elementNumber] = $this->arrays->getValue($cacheEntry, 'cache_id');

                        unset($sourceElementHashKeyChunk[$elementNumber]);
                    }
                }
            }
        }

        $this->logging->info(sprintf('Found %d cached source element(s)', count($this->sourceCachedElementNumbers)));
    }

    /**
     * @return string
     */
    abstract protected function getEntitiesLogName(): string;

    /**
     * @return void
     */
    private function displaySourceInvalidElements()
    {
        $this->logging->info(sprintf('Found %d invalid source element(s)', count($this->sourceInvalidElementNumbers)));

        foreach ($this->sourceInvalidElementNumbers as $elementNumber) {
            $this->logging->debug(sprintf('Found invalid source element: %s', $elementNumber));

            $element = $this->sourceData[$elementNumber];
            foreach ($this->sourceInvalidElementReasons[$elementNumber] as $elementReason) {
                $this->displaySourceInvalidElement(
                    $elementNumber,
                    $element,
                    $this->arrays->getValue($elementReason, static::REASON),
                    $this->arrays->getValue($elementReason, static::SEVERITY)
                );
            }
        }
    }

    /**
     * @param array $sourceData
     *
     * @return array
     */
    abstract protected function prepareSourceData(array $sourceData): array;

    /**
     * @param array $sourceData
     *
     * @return array
     */
    abstract protected function transformData(array $sourceData): array;

    /**
     * @param AdapterInterface $dbAdapter
     *
     * @return bool
     */
    abstract protected function validateTransformedData(AdapterInterface $dbAdapter): bool;

    /**
     * @return void
     * @throws Exception
     */
    protected function checkCachedTransformedData()
    {
        $this->transformedCacheIds = [];

        if (!$this->isUseTransformedDataCache()) {
            return;
        }

        foreach ($this->transformedData as $elementNumber => $element) {
            if (in_array($elementNumber, $this->transformedInvalidElementNumbers)) {
                continue;
            }

            $elementHashKey = $this->getTransformedElementHashKey($this->importHelper->elementToArray($element));
            $elementHashValue = $this->getTransformedElementHashValue($this->importHelper->elementToArray($element));

            if (!empty($elementHashKey) && !empty($elementHashValue)) {
                $this->transformedElementHashKeys[$elementNumber] = $elementHashKey;
                $this->transformedElementHashValues[$elementNumber] = $elementHashValue;
            }
        }

        $now = new DateTime();

        $transformedElementHashKeyChunks = array_chunk($this->transformedElementHashKeys, 1000, true);

        foreach ($transformedElementHashKeyChunks as $transformedElementHashKeyChunk) {
            $cacheEntryCollection = $this->importHelper->getTransformedCacheCollection();

            $cacheEntryCollection->addFieldToFilter('prefix', ['eq' => $this->transformedCachePrefix]);
            $cacheEntryCollection->addFieldToFilter('hash_key', ['in' => $transformedElementHashKeyChunk]);

            $this->logging->debug(sprintf('Executing query: %s', $cacheEntryCollection->getSelect()->assemble()));

            $cacheEntries = $this->databaseHelper->fetchAssoc($cacheEntryCollection->getSelect());

            foreach ($cacheEntries as $cacheEntry) {
                foreach ($transformedElementHashKeyChunk as $elementNumber => $elementHashKey) {
                    if (array_key_exists('hash_key', $cacheEntry) && $cacheEntry['hash_key'] === $elementHashKey) {
                        $expiresAt = DateTime::createFromFormat(
                            'Y-m-d H:m:s',
                            $this->arrays->getValue($cacheEntry, 'expires_at')
                        );

                        if (array_key_exists('data_hash', $cacheEntry)
                            && $cacheEntry['data_hash'] === $this->transformedElementHashValues[$elementNumber]
                            && $now->getTimestamp() < $expiresAt->getTimestamp()) {
                            $this->transformedCachedElementNumbers[] = $elementNumber;
                        }

                        $this->transformedCacheIds[$elementNumber] = $this->arrays->getValue($cacheEntry, 'cache_id');

                        unset($transformedElementHashKeyChunk[$elementNumber]);
                    }
                }
            }

            unset($cacheEntries);
        }

        $this->logging->info(
            sprintf(
                'Found %d cached transformed element(s)',
                count($this->transformedCachedElementNumbers)
            )
        );
    }

    /**
     * @return void
     */
    private function displayTransformedInvalidElements()
    {
        $this->logging->info(
            sprintf(
                'Found %d invalid transformed element(s)',
                count($this->transformedInvalidElementNumbers)
            )
        );

        foreach ($this->transformedInvalidElementNumbers as $elementNumber) {
            $this->logging->debug(sprintf('Found invalid transformed element: %s', $elementNumber));

            $element = $this->transformedData[$elementNumber];

            foreach ($this->transformedInvalidElementReasons[$elementNumber] as $elementReason) {
                $this->displayTransformedInvalidElement(
                    $elementNumber,
                    $element,
                    $this->arrays->getValue($elementReason, static::REASON),
                    $this->arrays->getValue($elementReason, static::SEVERITY)
                );
            }
        }
    }

    /**
     * @return void
     */
    private function saveSourceCache()
    {
        if (!$this->isUseSourceDataCache()) {
            return;
        }

        if (is_null($this->sourceCacheExpiry)) {
            $this->sourceCacheExpiry = new DateTime();
            $this->sourceCacheExpiry->add(new \DateInterval('P1Y'));
        }

        $sourceTransformedElementNumbers = [];

        foreach ($this->sourceTransformedRelations as $relation) {
            $sourceElementNumber = $this->arrays->getValue($relation, 'source');
            $transformedElementNumber = $this->arrays->getValue($relation, 'transformed');

            if (!array_key_exists($sourceElementNumber, $sourceTransformedElementNumbers)) {
                $sourceTransformedElementNumbers[$sourceElementNumber] = [];
            }

            if (!in_array($transformedElementNumber, $sourceTransformedElementNumbers[$sourceElementNumber])) {
                $sourceTransformedElementNumbers[$sourceElementNumber][] = $transformedElementNumber;
            }
        }

        $validSourceElementNumbers = [];

        foreach ($sourceTransformedElementNumbers as $sourceElementNumber => $transformedElementNumbers) {
            $valid = true;

            foreach ($transformedElementNumbers as $transformedElementNumber) {
                if (!in_array($transformedElementNumber, $this->transformedImportedElementNumbers)
                    && !in_array($transformedElementNumber, $this->transformedCachedElementNumbers)) {
                    $valid = false;
                }
            }

            if ($valid && !in_array($sourceElementNumber, $this->sourceCachedElementNumbers)) {
                $validSourceElementNumbers[] = $sourceElementNumber;
            }
        }

        $this->logging->info(sprintf('Saving %d source data hash(es)', count($validSourceElementNumbers)));

        foreach ($validSourceElementNumbers as $sourceElementNumber) {
            if (array_key_exists($sourceElementNumber, $this->sourceCacheIds)) {
                $cacheEntry = $this->importHelper->loadSourceCache($this->sourceCacheIds[$sourceElementNumber]);
            } else {
                if (!array_key_exists($sourceElementNumber, $this->sourceElementHashKeys)) {
                    $this->logging->warning(
                        sprintf(
                            'Could not save source data hash because no cache key was found for element with number: %d',
                            $sourceElementNumber
                        )
                    );

                    continue;
                }

                $cacheEntry = $this->importHelper->newSourceCache();

                $cacheEntry->setPrefix($this->sourceCachePrefix);
                $cacheEntry->setHashKey($this->sourceElementHashKeys[$sourceElementNumber]);
            }

            $cacheEntry->setDataHash($this->sourceElementHashValues[$sourceElementNumber]);
            $cacheEntry->setExpiresAt($this->sourceCacheExpiry->format('Y-m-d H:i:s'));

            $this->logging->debug(
                sprintf(
                    'Saving source data hash with prefix: %s and key: %s which expires at: %s',
                    $cacheEntry->getPrefix(),
                    $cacheEntry->getHashKey(),
                    $cacheEntry->getExpiresAt()
                )
            );

            if (!$this->isTest()) {
                try {
                    $this->importHelper->saveSourceCache($cacheEntry);
                } catch (Exception $exception) {
                    $this->logging->error(
                        sprintf(
                            'Could not save source data hash with prefix: %s and key: %s which expires at: %s because: %s',
                            $cacheEntry->getPrefix(),
                            $cacheEntry->getHashKey(),
                            $cacheEntry->getExpiresAt(),
                            $exception->getMessage()
                        )
                    );

                    $this->logging->error($exception);
                }
            }
        }
    }

    /**
     * @return void
     */
    private function saveTransformedCache()
    {
        if (!$this->isUseTransformedDataCache()) {
            return;
        }

        if (is_null($this->transformedCacheExpiry)) {
            $this->transformedCacheExpiry = new DateTime();
            $this->transformedCacheExpiry->add(new \DateInterval('P1Y'));
        }

        $validTransformedElementNumbers = [];

        foreach ($this->transformedImportedElementNumbers as $elementNumber) {
            if (array_key_exists($elementNumber, $this->transformedElementHashValues)
                && !in_array($elementNumber, $this->transformedCachedElementNumbers)) {
                $validTransformedElementNumbers[] = $elementNumber;
            }
        }

        $this->logging->info(sprintf('Saving %d transformed data hash(es)', count($validTransformedElementNumbers)));

        foreach ($validTransformedElementNumbers as $elementNumber) {
            if (array_key_exists($elementNumber, $this->transformedCacheIds)) {
                $cacheEntry = $this->importHelper->loadTransformedCache($this->transformedCacheIds[$elementNumber]);
            } else {
                if (!array_key_exists($elementNumber, $this->transformedElementHashKeys)) {
                    $this->logging->warning(
                        sprintf(
                            'Could not save transformed data hash because no cache key was found for element with number: %d',
                            $elementNumber
                        )
                    );

                    continue;
                }

                $cacheEntry = $this->importHelper->newTransformedCache();

                $cacheEntry->setPrefix($this->transformedCachePrefix);
                $cacheEntry->setHashKey($this->transformedElementHashKeys[$elementNumber]);
            }

            $cacheEntry->setDataHash($this->transformedElementHashValues[$elementNumber]);
            $cacheEntry->setExpiresAt($this->transformedCacheExpiry->format('Y-m-d H:i:s'));

            $this->logging->debug(
                sprintf(
                    'Saving transformed data hash with prefix: %s and key: %s which expires at: %s',
                    $cacheEntry->getPrefix(),
                    $cacheEntry->getHashKey(),
                    $cacheEntry->getExpiresAt()
                )
            );

            if (!$this->isTest()) {
                try {
                    $this->importHelper->saveTransformedCache($cacheEntry);
                } catch (Exception $exception) {
                    $this->logging->error(
                        sprintf(
                            'Could not save transformed data hash with prefix: %s and key: %s which expires at: %s because: %s',
                            $cacheEntry->getPrefix(),
                            $cacheEntry->getHashKey(),
                            $cacheEntry->getExpiresAt(),
                            $exception->getMessage()
                        )
                    );

                    $this->logging->error($exception);
                }
            }
        }
    }

    /**
     * @return bool
     */
    protected function isUseSourceDataCache(): bool
    {
        return $this->useSourceDataCache === true;
    }

    /**
     * @param bool $useSourceDataCache
     *
     * @return void
     */
    public function setUseSourceDataCache(bool $useSourceDataCache = true)
    {
        $this->useSourceDataCache = $useSourceDataCache;
    }

    /**
     * @param array $element
     *
     * @return string
     */
    abstract protected function getSourceElementHashKey(array $element): string;

    /**
     * @param mixed $element
     *
     * @return string
     */
    protected function getSourceElementHashValue($element): string
    {
        return md5(json_encode($element));
    }

    /**
     * @return string
     */
    public function getSourceCachePrefix(): string
    {
        return $this->sourceCachePrefix;
    }

    /**
     * @param string $sourceCachePrefix
     *
     * @return void
     */
    public function setSourceCachePrefix(string $sourceCachePrefix)
    {
        $this->sourceCachePrefix = $sourceCachePrefix;
    }

    /**
     * @param int    $sourceElementNumber
     * @param mixed  $sourceElement
     * @param string $reason
     * @param string $severity
     *
     * @return void
     */
    abstract protected function displaySourceInvalidElement(
        int $sourceElementNumber,
        $sourceElement,
        string $reason,
        string $severity = 'error'
    );

    /**
     * @return bool
     */
    protected function isUseTransformedDataCache(): bool
    {
        return $this->useTransformedDataCache === true;
    }

    /**
     * @param bool $useTransformedDataCache
     *
     * @return void
     */
    public function setUseTransformedDataCache(bool $useTransformedDataCache = true)
    {
        $this->useTransformedDataCache = $useTransformedDataCache;
    }

    /**
     * @param array $element
     *
     * @return string
     */
    abstract protected function getTransformedElementHashKey(array $element): string;

    /**
     * @param array $element
     *
     * @return string
     */
    protected function getTransformedElementHashValue(array $element): string
    {
        return md5(json_encode($element));
    }

    /**
     * @param int    $transformedElementNumber
     * @param mixed  $transformedElement
     * @param string $reason
     * @param string $severity
     *
     * @return void
     */
    abstract protected function displayTransformedInvalidElement(
        int $transformedElementNumber,
        $transformedElement,
        string $reason,
        string $severity = 'error'
    );

    /**
     * @param string $transformedCachePrefix
     *
     * @return void
     */
    public function setTransformedCachePrefix(string $transformedCachePrefix)
    {
        $this->transformedCachePrefix = $transformedCachePrefix;
    }

    /**
     * @return bool
     */
    public function isTest(): bool
    {
        return $this->test === true;
    }

    /**
     * @param bool $test
     *
     * @return void
     */
    public function setTest(bool $test = true)
    {
        $this->test = $test;
    }

    /**
     * @param mixed $sourceCacheExpiry
     *
     * @throws Exception
     */
    public function setSourceCacheExpiry($sourceCacheExpiry)
    {
        if ($sourceCacheExpiry instanceof DateTime) {
            $this->sourceCacheExpiry = $sourceCacheExpiry;
        } else {
            $this->sourceCacheExpiry = new DateTime();
            $this->sourceCacheExpiry->add(new \DateInterval(sprintf('P%dS', $sourceCacheExpiry)));
        }
    }

    /**
     * @param mixed $transformedCacheExpiry
     *
     * @throws Exception
     */
    public function setTransformedCacheExpiry($transformedCacheExpiry)
    {
        if ($transformedCacheExpiry instanceof DateTime) {
            $this->transformedCacheExpiry = $transformedCacheExpiry;
        } else {
            $this->transformedCacheExpiry = new DateTime();
            $this->transformedCacheExpiry->add(new \DateInterval(sprintf('P%dS', $transformedCacheExpiry)));
        }
    }

    /**
     * @param int    $elementNumber
     * @param string $reason
     * @param string $severity
     *
     * @return  void
     */
    protected function addTransformedInvalidElementReason(
        int $elementNumber,
        string $reason,
        string $severity = 'error'
    ) {
        $this->logging->debug(
            sprintf(
                'Marking transformed element with number: %d as invalid with reason: %s',
                $elementNumber,
                $reason
            )
        );

        if (!in_array($elementNumber, $this->transformedInvalidElementNumbers)) {
            $this->transformedInvalidElementNumbers[] = $elementNumber;
        }

        if (!array_key_exists($elementNumber, $this->transformedInvalidElementReasons)) {
            $this->transformedInvalidElementReasons[$elementNumber] = [];
        }

        $invalidElement = [
            static::REASON   => $reason,
            static::SEVERITY => $severity
        ];

        $this->transformedInvalidElementReasons[$elementNumber][] = $invalidElement;
    }

    /**
     * @param int    $elementNumber
     * @param string $reason
     * @param string $severity
     */
    protected function addSourceInvalidElementReason(int $elementNumber, string $reason, string $severity = 'error')
    {
        $this->logging->debug(
            sprintf(
                'Marking source element with number: %d as invalid with reason: %s',
                $elementNumber,
                $reason
            )
        );

        if (!in_array($elementNumber, $this->sourceInvalidElementNumbers)) {
            $this->sourceInvalidElementNumbers[] = $elementNumber;
        }

        if (!array_key_exists($elementNumber, $this->sourceInvalidElementReasons)) {
            $this->sourceInvalidElementReasons[$elementNumber] = [];
        }

        $invalidElement = [
            static::REASON   => $reason,
            static::SEVERITY => $severity
        ];

        $this->sourceInvalidElementReasons[$elementNumber][] = $invalidElement;
    }

    /**
     * @return array
     */
    public function getTransformedInvalidElementsData(): array
    {
        $result = [];

        foreach ($this->transformedInvalidElementNumbers as $elementNumber) {
            $result[$elementNumber] = $this->transformedData[$elementNumber];
        }

        return $result;
    }

    /**
     * @param int $sourceElementNumber
     * @param int $transformedElementNumber
     *
     * @return  void
     */
    protected function addSourceTransformedRelation(int $sourceElementNumber, int $transformedElementNumber)
    {
        $relation = [
            'source'      => $sourceElementNumber,
            'transformed' => $transformedElementNumber
        ];

        $this->sourceTransformedRelations[] = $relation;
    }

    /**
     * @param int $elementNumber
     *
     * @return bool
     */
    protected function isSourceInvalidElementNumber(int $elementNumber): bool
    {
        return in_array($elementNumber, $this->sourceInvalidElementNumbers);
    }

    /**
     * @return bool
     */
    public function hasErrors(): bool
    {
        return !empty($this->sourceInvalidElementNumbers) || !empty($this->transformedInvalidElementNumbers);
    }

    /**
     * @param array    $element
     * @param int|null $elementNumber
     */
    protected function addTransformedData(array $element, int $elementNumber = null)
    {
        if ($elementNumber === null) {
            $this->transformedData[] = $element;
        } else {
            $this->transformedData[$elementNumber] = $element;
        }
    }

    /**
     * @param int $elementNumber
     */
    protected function addTransformedUnchangedElementNumber(int $elementNumber)
    {
        $this->transformedUnchangedElementNumbers[] = $elementNumber;
    }

    /**
     * @param int $elementNumber
     */
    protected function removeTransformedUnchangedElementNumbers(int $elementNumber)
    {
        unset($this->transformedUnchangedElementNumbers[$elementNumber]);
    }

    /**
     * @param int $elementNumber
     */
    protected function addTransformedChangedElementNumbers(int $elementNumber)
    {
        $this->transformedChangedElementNumbers[] = $elementNumber;
    }

    /**
     * @param int $elementNumber
     */
    protected function addTransformedImportedElementNumbers(int $elementNumber)
    {
        $this->transformedImportedElementNumbers[] = $elementNumber;
    }

    /**
     * @return array
     */
    public function getTransformedInvalidElementNumbers(): array
    {
        return $this->transformedInvalidElementNumbers;
    }

    /**
     * @return array
     */
    protected function getTransformedCachedElementNumbers(): array
    {
        return $this->transformedCachedElementNumbers;
    }

    /**
     * @return array
     */
    protected function getTransformedImportedElementNumbers(): array
    {
        return $this->transformedImportedElementNumbers;
    }

    /**
     * @return array
     */
    protected function getTransformedUnchangedElementNumbers(): array
    {
        return $this->transformedUnchangedElementNumbers;
    }

    /**
     * @return array
     */
    public function getTransformedData(): array
    {
        return $this->transformedData;
    }

    /**
     * @return array
     */
    public function getAssociatedItemModels(): array
    {
        return $this->associatedItemModels;
    }

    /**
     * @param string $key
     * @param string $model
     *
     * @return void
     */
    public function addAssociatedItemModel(string $key, string $model)
    {
        $this->associatedItemModels[$key] = $model;
    }

    /**
     * @return  void
     */
    abstract protected function cleanup();
}

<?php

declare(strict_types=1);

/**
 * @author      Andreas Knollmann
 * @copyright   Copyright (c) 2014-2026 Softwareentwicklung Andreas Knollmann
 * @license     http://www.opensource.org/licenses/mit-license.php MIT
 */

namespace Infrangible\Import\Model;

use Magento\Framework\DB\Adapter\AdapterInterface;
use Psr\Log\LoggerInterface;

/**
 * A value of an element of an import that refers to other data, for example to other entities, validated with the
 * element.
 */
abstract class Related
{
    /** @var list<string> */
    private array $invalidReasons = [];

    /**
     * @param LoggerInterface $logging
     * @param Import $importer
     */
    public function __construct(
        protected LoggerInterface $logging,
        protected Import $importer
    ) {
    }

    /**
     * Returns the import of the value.
     *
     * @return Import
     */
    protected function getImporter(): Import
    {
        return $this->importer;
    }

    /**
     * Validates the value for the element and adds the reasons why it is invalid with addInvalidReason().
     *
     * @param AdapterInterface $dbAdapter
     * @param int $storeId
     * @param array $element
     * @phpstan-param array<int|string, mixed> $element
     *
     * @return void
     */
    abstract public function validate(AdapterInterface $dbAdapter, int $storeId, array $element): void;

    /**
     * Adds a reason why the value is invalid.
     *
     * @param string $reason
     *
     * @return void
     */
    protected function addInvalidReason(string $reason): void
    {
        $this->invalidReasons[] = $reason;
    }

    /**
     * Checks if the value is valid.
     *
     * @return bool
     */
    public function isValid(): bool
    {
        return count($this->invalidReasons) === 0;
    }

    /**
     * Returns the reasons why the value is invalid.
     *
     * @return string
     */
    public function getInvalidReason(): string
    {
        return implode('; ', $this->invalidReasons);
    }

    /**
     * Returns the value as array, for the hash of the element.
     *
     * @return array
     * @phpstan-return array<int|string, mixed>
     */
    abstract public function toArray(): array;
}

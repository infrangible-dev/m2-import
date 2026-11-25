<?php

declare(strict_types=1);

/**
 * @author      Andreas Knollmann
 * @copyright   Copyright (c) 2014-2026 Softwareentwicklung Andreas Knollmann
 * @license     http://www.opensource.org/licenses/mit-license.php MIT
 */

namespace Infrangible\Import\Test\Unit\Model;

use Infrangible\Import\Model\Import;
use Magento\Framework\DB\Adapter\AdapterInterface;

/**
 * An import that transforms the names of the source elements to upper case and imports every transformed element.
 */
class TestImport extends Import
{
    /** @var array<int, array{sku: string, name: string}> */
    public array $source = [];

    /** @var list<string> SKUs of the imported elements */
    public array $imported = [];

    /** @var list<string> invalid elements with reason */
    public array $invalid = [];

    /**
     * @inheritDoc
     */
    protected function prepare(): void
    {
    }

    /**
     * @inheritDoc
     */
    protected function cleanup(): void
    {
    }

    /**
     * @inheritDoc
     */
    protected function getEntitiesLogName(): string
    {
        return 'test elements';
    }

    /**
     * @inheritDoc
     */
    protected function readSourceData(): array
    {
        return $this->source;
    }

    /**
     * @inheritDoc
     */
    protected function validateSourceData(array $sourceData): void
    {
        foreach ($sourceData as $number => $element) {
            if (is_array($element) && ($element[ 'name' ] ?? '') === '') {
                $this->addSourceInvalidElementReason((int)$number, 'name missing');
            }
        }
    }

    /**
     * @inheritDoc
     */
    protected function prepareSourceData(array $sourceData): array
    {
        return $sourceData;
    }

    /**
     * @inheritDoc
     */
    protected function transformData(array $sourceData): array
    {
        $transformed = [];

        foreach ($sourceData as $number => $element) {
            if (is_array($element)) {
                $transformed[ $number ] = ['sku' => $element[ 'sku' ] ?? '', 'name' => $element[ 'name' ] ?? ''];

                $this->addSourceTransformedRelation((int)$number, (int)$number);
            }
        }

        return $transformed;
    }

    /**
     * @inheritDoc
     */
    protected function validateTransformedData(AdapterInterface $dbAdapter): bool
    {
        return true;
    }

    /**
     * @inheritDoc
     */
    protected function importTransformedData(AdapterInterface $dbAdapter): void
    {
        foreach ($this->getTransformedData() as $number => $element) {
            if (is_array($element)) {
                $this->imported[] = (string)($element[ 'sku' ] ?? '');

                $this->addTransformedImportedElementNumbers((int)$number);
            }
        }
    }

    /**
     * @inheritDoc
     */
    protected function getSourceElementHashKey(array $sourceElement): string
    {
        $sku = $sourceElement[ 'sku' ] ?? '';

        return is_string($sku) ? $sku : '';
    }

    /**
     * @inheritDoc
     */
    protected function getTransformedElementHashKey(array $element): string
    {
        $sku = $element[ 'sku' ] ?? '';

        return is_string($sku) ? $sku : '';
    }

    /**
     * @inheritDoc
     */
    protected function displaySourceInvalidElement(
        int $sourceElementNumber,
        mixed $sourceElement,
        string $reason,
        string $severity = 'error'
    ): void {
        $this->invalid[] = $sourceElementNumber . ': ' . $reason;
    }

    /**
     * @inheritDoc
     */
    protected function displayTransformedInvalidElement(
        int $transformedElementNumber,
        mixed $transformedElement,
        string $reason,
        string $severity = 'error'
    ): void {
        $this->invalid[] = $transformedElementNumber . ': ' . $reason;
    }

    /**
     * Marks the element numbers as unchanged.
     *
     * @param int[] $elementNumbers
     *
     * @return void
     */
    public function markUnchanged(array $elementNumbers): void
    {
        foreach ($elementNumbers as $elementNumber) {
            $this->addTransformedUnchangedElementNumber($elementNumber);
        }
    }

    /**
     * Removes the mark of the element number as unchanged.
     *
     * @param int $elementNumber
     *
     * @return void
     */
    public function unmarkUnchanged(int $elementNumber): void
    {
        $this->removeTransformedUnchangedElementNumbers($elementNumber);
    }

    /**
     * Returns the element numbers marked as unchanged.
     *
     * @return int[]
     */
    public function getUnchanged(): array
    {
        return $this->getTransformedUnchangedElementNumbers();
    }
}

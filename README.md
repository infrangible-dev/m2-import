# Infrangible Import

The base of data imports, for example from an ERP or PIM system. The module imports nothing by itself; import
routines extend its class `Model\Import` and describe where their data comes from and how it maps to Magento.
[Infrangible Import Entity](https://www.infrangible.dev/modules/import-entity) builds on it to import Magento entities.

## Steps of an import

`Import::run()` calls `prepare()`, the import and `cleanup()`. The import:

1. reads the source elements (`readSourceData()`) and validates them (`validateSourceData()`),
2. skips the source elements that are cached unchanged, prepares (`prepareSourceData()`) and transforms
   (`transformData()`) the others,
3. validates the transformed elements (`validateTransformedData()`, in a transaction),
4. skips the transformed elements that are cached unchanged and imports the others (`importTransformedData()`),
5. saves the hashes of the imported elements in the caches.

Invalid elements are marked with a reason and a severity (`addSourceInvalidElementReason()`,
`addTransformedInvalidElementReason()`) and skipped; the routine shows them (`displaySourceInvalidElement()`,
`displayTransformedInvalidElement()`).

## Caches

With `setUseSourceDataCache()` and `setUseTransformedDataCache()` the hashes of the elements are stored in the tables
`import_source_cache` and `import_transformed_cache`, by a prefix of the routine and a key of the element
(`getSourceElementHashKey()`, `getTransformedElementHashKey()`). Elements whose hash did not change and whose entry has
not expired are skipped. Entries expire after a year, or at the date or after the number of seconds set with
`setSourceCacheExpiry()` and `setTransformedCacheExpiry()`. A source element is cached only when all elements
transformed from it (`addSourceTransformedRelation()`) were imported or cached. In test mode (`setTest()`), no hashes
are saved and no attribute options are created.

## Attribute values

The helper `Helper\Data` validates attribute values for the type of their column (`validateAttribute()`), adds unknown
values of attributes with options as options, if asked, and creates the updates of an attribute value
(`createAttributeUpdates()`): for product and category attributes the values of the store, of all stores of the
website or of the admin store, depending on the scope of the attribute; prices are global if the catalog prices are
global.

The module also adds the column `hash` to the table of the product images (`catalog_product_entity_media_gallery`),
for routines that recognize unchanged images.

## Requirements

- Magento Open Source or Adobe Commerce 2.4.6 or newer
- PHP 8.2 or newer

## Installation

```bash
composer require infrangible/m2-import
bin/magento module:enable Infrangible_Import
bin/magento setup:upgrade
```

In production mode, also run `bin/magento setup:di:compile`.

## License

Infrangible Import is licensed under the MIT License - see the LICENSE file for details.

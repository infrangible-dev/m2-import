# Changelog

## 2.0.1

### Changed

- Supports PHP 8.1 again, like Magento 2.4.6: the Adobe Commerce Marketplace requires modules to allow every PHP
  version of the Magento versions they support.

## 2.0.0

### Breaking changes

- Requires Magento 2.4.6 or newer and PHP 8.2 or newer. Older versions of Magento and PHP 7.1 to 8.1 are no longer
  supported; use version 1.x for these.
- Requires `infrangible/m2-core` 5.0, `infrangible/m2-foundation` 3.0 and `fewedev/base` 2.4.3.
- All classes have native types and use constructor property promotion. The abstract methods of `Model\Import` and
  `Model\Related` declare their return types, so routines that extend them have to declare them too. The constructors
  of `Model\Import` and `Model\Related` keep their parameters; `Helper\Data` takes `FeWeDev\Base\Variables` as well.
- The hashes of the elements are SHA-256 instead of MD5: the first import after the update finds no unchanged elements
  in the caches and imports all elements once.
- `Helper\Data::clearCacheSource()` and `clearCacheTransformed()` return whether an entry was deleted.
- The tables are declared in `etc/db_schema.xml` instead of `Setup/InstallSchema.php`; `Setup/Uninstall.php` was
  removed, Magento removes declared tables itself. The unique key and the index of the cache tables get Magento's
  names; `updated_at` is updated when an entry changes.

### Fixed

- Expired cache entries were still used: their expiry was read with the month in place of the minutes. In a test, an
  entry that had expired an hour before was skipped as unchanged; now it is imported again.
- `setSourceCacheExpiry()` and `setTransformedCacheExpiry()` with a number of seconds failed with "Unknown or bad
  format".
- `removeTransformedUnchangedElementNumbers()` removed the element at the position of the number instead of the
  number.
- Validation results were cached by attribute and value only, so a value that was validated (and maybe added as
  option) for one entity type or store counted as valid for all others.
- Special attributes of an unknown type printed their value with `var_dump()`; now it is returned unchanged.

### Added

- Unit tests.
- README with the steps of an import, the caches and the attribute values.

### Changed

- The lookups and saves of the source and transformed caches share their code.
- Checks with PHPStan level 9, the Magento coding standard and unit tests on every push.

# ERDERA-RD3 Expressions

## Individuals: Year of birth validation

This validation expression determines if the year (a number) is
greater than the current year or earlier than fixed year. By default,
the earliest year allowed is 1900, and the current year is based on
the UTC date.

Input value must be in four digit year format (`YYYY`). If the value
is not within range, an error message will be thrown when data is
imported in a file or when it is entered into the web form.

**Arguments**:

| Param | Type | Description |
| --- | --- | --- |
| yearOfBirth | `number` | a year as four digits (YYYY) |

**Example**  
To use this expression in your schema, copy the contents of the
js file into your molgenis schema. Call the function and enter
the name of the column that corresponds to "year of birth".

```js
// contents of this script
function validateYearOfBirth (yearh) {
    // ....
}

// name of the column containing the value to validate
validateYearOfBirth(yearOfBirth)
```

## Samples: Concentration validation

This expression validates the input of the concentration column.
Concentration measures are defined by the study protocols and
must be between 90 ng/µl and 150 ng/µl. If the value is outside
of this range, than an error message will be thrown on import
or entry into the webform.

**Arguments**:

| Param | Type | Description |
| --- | --- | --- |
| concentration | `decimal` | concentration measurement (ng/µl) |

**Example**  

```js
// in your schema

function validateConcentration(value) {
   // ...
}

// name of the column containing the value to validate
validateConcentration(concentration)
```

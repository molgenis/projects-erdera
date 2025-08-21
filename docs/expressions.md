# RD3 expressions documentation

## Individuals Year of birth validation

This validation expresses determines if the year&mdash;an integer&mdash;is
greater than the current year or earlier than fixed year (which is set to 1900).
This forces users to enter year as `YYYY`, and if the input is not within range,
an error will be thrown.

| Param | Type | Description |
| --- | --- | --- |
| yearOfBirth | `number` | a year as four digits (YYYY) |

**Example**  

```js
// in the schema
function validateYearOfBirth (yearOfBirth) {
    // ....
}
validateYearOfBirth(name)
```

## Samples Concentration validation

Validation of user input in the concentration column

| Param | Type | Description |
| --- | --- | --- |
| concentration | `number` | a concentration as a decimal |

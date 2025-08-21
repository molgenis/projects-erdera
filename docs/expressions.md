# Expressions documentation

<dl>
<dt><a href="#Individuals Year of birth validation">Individuals Year of birth validation</a></dt>
<dd><p>This validation expresses determines if the year&mdash;an integer&mdash;is
greater than the current year or earlier than fixed year (which is set to 1900).
This forces users to enter year as `YYYY`, and if the input is not within range,
an error will be thrown.</p>
</dd>
<dt><a href="#Samples Concentration validation">Samples Concentration validation</a></dt>
<dd><p>Validation of user input in the concentration column</p>
</dd>
</dl>

<a name="Individuals Year of birth validation"></a>

## Individuals Year of birth validation

This validation expresses determines if the year&mdash;an integer&mdash;is
greater than the current year or earlier than fixed year (which is set to 1900).
This forces users to enter year as `YYYY`, and if the input is not within range,
an error will be thrown.

**Kind**: global variable  
**Tag**: Individuals.year_of_birth.validation  

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

<a name="Samples Concentration validation"></a>

## Samples Concentration validation

Validation of user input in the concentration column

**Kind**: global variable  
**Tag**: Samples.concentration.validation  

| Param | Type | Description |
| --- | --- | --- |
| concentration | `number` | a concentration as a decimal |

# Action Plan

## Data Cleansing
We need to check if all data in the dataset is valid and can be parsed by our code. Ideally, we would like to parse each row into its own class and access each attribute within the class via a map. Thus, the approach we are taking would be to run several checks on each of our dataset.

#### Step 1: Check if dataset follows XML Format
We want to check whether the dataset that we have follows the same format as we have envisioned. For example, we would like the `Posts.xml` dataset to follow the similar format:
```xml
<?xml version="1.0" encoding="utf-8"?>
<posts>
    <row Id="4" PostTypeId="1" ... />
    <row Id="6" PostTypeId="1" ... />
    ...
    <row Id="7" PostTypeId="2" ... />
</post>
```
These are the set of rules we can apply:
 - Check if the XML file starts with the approriate header and ends with the approriate footer. The header could be the XML version as well as the opening and closing tags og the xml.
 - Check if each row contains a valid and unique ID. This approach can be done be collating all the row IDs as key/value pair and check if the length of the result set of key/value pair is equal the the number of posts given (length of the number of valid posts rows)


 ## Indexing dataset
 This step is crucial for a search engine for allowing fast access to the data set essentially acheiving a near `O(1)` time complexitiy

# SRM
SRM is System Relation Mapping Framework.

## ER
The ER diagram showing the whole picture is shown below.
The diagram below is for conceptual purposes only and may differ slightly from the physical name.

### System table
```mermaid
erDiagram
    INTERLINK_DESTINATION ||..o{ INTERLINK_DATASOURCE : INTERLINK_DESTINATION_ID
    INTERLINK_DESTINATION ||..o{ INTERLINK_TRANSACTION : INTERLINK_DESTINATION_ID

    INTERLINK_DATASOURCE ||..o{ INTERLINK_PROCESS : INTERLINK_DATASOURCE_ID

    INTERLINK_TRANSACTION ||..o{ INTERLINK_PROCESS : INTERLINK_TRANSACTION_ID

    INTERLINK_DESTINATION{
        int INTERLINK_DESTINATION_ID PK
        string TABLE_FULLNAME
        json DB_TABLE
        json DB_SEQUENCE
        json REVERSE_OPTION
    }

    INTERLINK_DATASOURCE{
        int INTERLINK_DATASOURCE_ID PK
        int INTERLINK_DESTINATION_ID FK
        string DATASOURCE_NAME
        string KEY_NAME
        string QUERY
        json KEY_COLUMNS
    }

    INTERLINK_TRANSACTION{
        int INTERLINK_TRANSACTION_ID PK
        int INTERLINK_DESTINATION_ID FK
        string SERVICE_NAME
        string ARGUMENT
    }

    INTERLINK_PROCESS{
        int INTERLINK_PROCESS_ID PK
        int INTERLINK_TRANSACTION_ID FK
        int INTERLINK_DATASOURCE_ID FK
        string ACTION_NAME
        int INSERT_COUNT
    }
```

### Process <-> Destination
```mermaid
erDiagram
    INTERLINK_DATASOURCE ||..o{ INTERLINK_PROCESS : INTERLINK_DATASOURCE_ID

    INTERLINK_PROCESS ||..o{ destination_a__RELATION : INTERLINK_PROCESS_ID
    destination_a__RELATION |o..|| destination_a : destination_a_id

    INTERLINK_PROCESS ||..o{ destination_n__RELATION : INTERLINK_PROCESS_ID
    destination_n__RELATION |o..|| destination_n : destination_n_id

    INTERLINK_PROCESS{
        int INTERLINK_PROCESS_ID PK
        int INTERLINK_DATASOURCE_ID FK
    }

    destination_a__RELATION{
        int destination_a_id PK
        int INTERLINK_PROCESS_ID FK
        int ROOT__destination_a_id
        int ORIGIN__destination_a_id
        string INTERLINK_REMARKS
    }

    INTERLINK_DATASOURCE{
        int INTERLINK_DATASOURCE_ID PK
        string KEY_NAME
    }

    destination_a{
        int destination_a_id PK
        int number1
        int number2
        int number3
    }

    destination_n__RELATION{
        int destination_n_id PK
        int INTERLINK_PROCESS_ID FK
        int ROOT__destination_n_id
        int ORIGIN__destination_n_id
        string INTERLINK_REMARKS
    }

    destination_n{
        int destination_n_id PK
        any values
    }
```

### Destination <-> Datasource

```mermaid
erDiagram
    destination_a ||..o| destination_a__KEY_M_datasource_x : destination_a_id
    destination_a ||..o| destination_a__KEY_R_datasource_x : destination_a_id
    destination_a__KEY_M_datasource_x |o..||datasource_x : datasource_x_id
    destination_a__KEY_R_datasource_x }o..||datasource_x : datasource_x_id

    destination_a ||..o| destination_a__KEY_M_datasource_y : destination_a_id
    destination_a ||..o| destination_a__KEY_R_datasource_y : destination_a_id
    destination_a__KEY_M_datasource_y |o..||datasource_y : datasource_y_id
    destination_a__KEY_R_datasource_y }o..||datasource_y : datasource_y_id

    destination_a {
        int destination_a_id PK
    }

    destination_a__KEY_M_datasource_x {
        int dataource_x_id PK
        int destination_a_id FK
    }

    destination_a__KEY_R_datasource_x {
        int destination_a_id PK
        int dataource_x_id FK
    }

    datasource_x {
        int datasource_x_id PK
        any values
    }

    destination_a__KEY_M_datasource_y {
        int dataource_y_id PK
        int destination_a_id FK
    }

    destination_a__KEY_R_datasource_y {
        int destination_a_id PK
        int dataource_y_id FK
    }

    datasource_y {
        int datasource_y_id PK
        any values
    }
```



```mermaid
erDiagram
    datasource_x ||..o| x_KEY_MAP : DATASOURCE_X_KEY
    datasource_x ||..o{ x_KEY_RELATION : DATASOURCE_X_KEY
    x_KEY_MAP |o--|| destination_a : DESTINATION_A_SEQ
    x_KEY_RELATION }|--|| destination_a : DESTINATION_A_SEQ

    destination_a ||--|| a_RELATION : DESTINATION_A_SEQ
    a_RELATION }|--|| INTERLINK_PROCESS : INTERLINK_PROCESS_ID
    INTERLINK_PROCESS }|--|| INTERLINK_TRANSACTION : INTERLINK_TRANSACTION_ID

  datasource_x {
  }

  x_KEY_MAP {
    int DESTINATION_A_SEQ PK
    int DATASOURCE_X_KEY FK
  }

  x_KEY_RELATION {
    int DESTINATION_A_SEQ PK
    int DATASOURCE_X_KEY FK
  }

  destination_a {
  }

  a_RELATION{
    int DESTINATION_A_SEQ PK
    int ROOT_DESTINATION_SEQ
    int ORIGIN_DESTINATION_SEQ
    int INTERLINK_PROCESS_ID FK
    string REMARKS
  }

  INTERLINK_PROCESS{
    int INTERLINK_PROCESS_ID PK
    int INTERLINK_TRANSACTION_ID FK
    int INTERLINK_DATASOURCE_ID FK
    string ACTION_NAME
    int INSERT_COUNT
  }

  INTERLINK_TRANSACTION{
    int INTERLINK_TRANSACTION_ID PK
    int INTERLINK_DESTINATION_ID FK
    string SERVICE_NAME
    string ARGUMENT
  }
```

### datasource_x
"datasource_x" is the source table, query, or view. This is called a datasource. The primary key is DATASOURCE_X_KEY. There are no restrictions on the type of primary key. Composite keys are also supported.

### destination_a
"destination_a" is the destination table. This is called a dstination. The primary key is DESTINATION_A_SEQ. Primary key must be a sequence.

### x_KEY_MAP, x_KEY_RELATION
"x_KEY_MAP" and "x_KEY_RELATION" are the primary key conversion tables for datasource and destination. Typically, there is a one-to-one relationship between datasource and destination. However, if there is a correction, the data will be transferred with its sign reversed. Therefore, it may be transferred multiple times. x_KEY_RELATION stores the key conversion table for all transfers, and x_KEY_MAP stores only the last key conversion table.

### a_RELATION
"a_RELATION" stores relation information within destination. Normally, there is one line for the destination, but if sign-reversal transfer occurs due to correction, there will be N lines. The original ID is recorded in ROOT_DESTINATION_A_SEQ. ORIGIN_DESTINATION_A_SEQ records the ID that is the source of sign inversion.

### INTERLINK_PROCESS
INTERLINK_PROCESS indicates the transfer process. It has the transfer source, transfer destination, action name, number of added items, and table name of the key conversion table.

NOTE: Regarding the reason for having key conversion assertions. The relationship between datasource and destination is N:1. To support multiple datasources, a key conversion table is required for each datasource. To reversely look up datasource from destination, you need to perform an outer join with all key conversion tables. This is not realistic. Therefore, we use INTERLINK_PROCESS to manage the table name itself, which tells us which key conversion table to refer to.

### INTERLINK_TRANSACTION
INTERLINK_TRANSACTION, as the name suggests, indicates a transaction.

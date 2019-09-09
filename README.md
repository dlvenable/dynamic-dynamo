# Dynamic Dynamo
Provides higher-level abstractions for DynamoDB using the Amazon AWS Java SDK

**Deprecated**: The AWS Java SDK has much better APIs now. And using CloudFormation to create DynamoDB tables is much better.

## Building Tables

One of the primary features of this project is a fluent syntax for creating DynamoDB tables.

### Example

```
new TableBuilder()
        .name("MyGreatNewTable")
        .primary()
            .hash()
                .name("HashId").type(ScalarAttributeType.S)
            .range()
                .name("RangeId").type(ScalarAttributeType.S)
            .readCapacity(2)
            .writeCapacity(2)
            .and()
        .create(amazonDynamoDB);
```

## Table Creator

There is also a utility function to create tables.

```
TableHelper.createTableIfNecessary(amazonDynamoDD, "MyGreatNewTable", new MyGreatTableDefiner());

private class MyGreatTableDefiner implements TableDefiner
{
    @Override
    public void defineTable(TableBuilder tableBuilder) {
        tableBuilder
          .primary()
              .hash()
                  .name("HashId").type(ScalarAttributeType.S)
              .range()
                  .name("RangeId").type(ScalarAttributeType.S)
              .readCapacity(2)
              .writeCapacity(2)
    }
}
```

## Download

This project is available in Maven Central

```
repositories {
    mavenCentral()
}

dependencies {
    compile("io.venable.amazonaws:dynamic-dynamo:0.3")
}
```

# Oden

## Overview

Oden is a Bitcask implementation, which is a high-performance key-value store written in Java.

## Features

- Fast read and write operations
- Simple design
- Efficient data storage

## Installation

To install Oden, follow these steps:

1. Clone the repository:

  ```sh
  git clone https://github.com/maelstrom0x8/oden.git
  ```

2. Navigate to the project directory:

  ```sh
  cd oden
  ```

3. Build the project using Maven:

  ```sh
  mvn clean install
  ```

## Usage

Here's a basic example of how to use Oden in Java:

```java
import io.maelstrom.oden.OdenOptions;
import io.maelstrom.oden.Oden;

{
    var options = OdenOptions("/path/to/data/directory");
    var oden = Oden.open(options);
    
    oden.Put("key", "value");
    // Get a value by key
    String value = oden.Get("key");
    
    // Delete a key-value pair
    oden.Delete("key");

}
```

## Contributing

Contributions are welcome! Please read the [contributing guidelines](CONTRIBUTING.md) first.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

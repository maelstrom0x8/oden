# Oden

This project aims to build a key-value store from scratch using the Bitcask storage model. It serves as an educational endeavor to grasp some fundamentals of data storage systems. The key-value store supports basic put/get operations, writes data to append-only files, guarantees data durability, and performs maintenance through merging processes. Key operations occur in a few important ways: every write goes to an append-only file, and an in-memory index is maintained to track where the value for each key resides. When necessary, a merge process is used to clean up. This is a learning project and a work in progress, built to understand storage systems better, so expect some rough edges! Currently, the focus is on implementing the merge process and adding better configuration options.

## Getting Started

After building the project, you can use Oden in your Java application like this (see [Installation](#installation) for build instructions):

```java
import io.maelstrom.oden.OdenOptions;
import io.maelstrom.oden.Oden;

{
    var options = OdenOptions.Default(); // This will create a new Oden instance with default options
    Oden oden = Oden.open(options);
    
    oden.Put("key", "value");
    // Get a value by key
    String value = oden.Get("key");
    
    // Delete a key-value pair
    oden.Delete("key");

}
```

## References

Check out these resources that inspired this project:
- [Bitcask paper](https://riak.com/assets/bitcask-intro.pdf)
- [Martin Kleppmann's "Designing Data-Intensive Applications"](https://dataintensive.net/)

## [Installation](#installation)

### Prerequisites
Make sure you have the following installed:
- Java 21
- Maven 4+

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

## Contributing

Feel free to open issues or PRs if you spot something interesting! This is a learning project, so I'm open to suggestions and improvements.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

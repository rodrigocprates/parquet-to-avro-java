import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

import static java.util.Optional.empty;
import static java.util.Optional.of;

public class Main {

    public static void main(String[] args) throws IOException {

        if (args.length < 2)
            throw new RuntimeException("You must inform at least 2 parameters: 1st=inputFilePath(.parquet), 2nd=outputFilePath(.avro), 3rd=avroSchemaPath(.avsc -> *optional*)");

        String inputFile = args[0];
        String outputFile = args[1];
        Optional<String> avroSchemaFile = args.length > 2 ? of(args[3]) : empty();

        // Prepare to read .parquet
        HadoopInputFile hadoopInputFile = HadoopInputFile.fromPath(new Path(inputFile), new Configuration());
        ParquetReader<GenericRecord> parquetReader = AvroParquetReader.<GenericRecord>builder(hadoopInputFile).build();

        // Set avro schema (infer schema from Parquet or use .avsc file provided)
        Schema avroSchema = avroSchemaFile
                .map(Main::readAvroSchema)
                .orElse(parquetReader.read().getSchema());

        // Write to .avro
        writeToAvro(parquetReader, avroSchema, new File(outputFile));
    }

    private static Schema readAvroSchema(String avroSchemaFileName) {
        try {
            return new Schema.Parser().parse(new File(avroSchemaFileName));
        } catch (IOException e) {
            throw new RuntimeException(String.format("Could not load avro schema from path [%s]", avroSchemaFileName), e);
        }
    }

    private static void writeToAvro(final ParquetReader<GenericRecord> parquetReader, final Schema schema, final File outputFile) {
        final DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        try (DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<>(writer)) {
            fileWriter.create(schema, outputFile);

            GenericRecord nextRecord;
            do {
                nextRecord = parquetReader.read();
                if (nextRecord != null)
                    fileWriter.append(nextRecord);
            } while (nextRecord != null);

        } catch (Exception e) {
            throw new RuntimeException("Could not write to file", e);
        } finally {
            try {
                parquetReader.close();
            } catch (IOException e) {
                throw new RuntimeException("Could not close reader stream", e);
            }
        }
    }

    private static Schema mockAvroSchema() {
        // for testing purposes
        final Schema schema = SchemaBuilder
                .record("sample")
                .fields()
                .name("id").type().intType().noDefault()
                .name("product").type().stringType().stringDefault("null")
                .name("price").type().intType().noDefault()
                .name("available").type().intType().intDefault(0)
                .endRecord();
        return schema;
    }

}

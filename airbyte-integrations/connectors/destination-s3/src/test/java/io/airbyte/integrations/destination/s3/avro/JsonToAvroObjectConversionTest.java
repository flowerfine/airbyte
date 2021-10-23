package io.airbyte.integrations.destination.s3.avro;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.airbyte.commons.jackson.MoreMappers;
import io.airbyte.commons.resources.MoreResources;
import io.airbyte.commons.util.MoreIterators;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;
import tech.allegro.schema.json2avro.converter.JsonHelper;

/**
 * This file tests both the schema and object converter.
 */
public class JsonToAvroObjectConversionTest {

  private static final ObjectWriter WRITER = MoreMappers.initMapper().writer();
  private static final JsonToAvroSchemaConverter SCHEMA_CONVERTER = new JsonToAvroSchemaConverter();

  public static class JsonToAvroConverterTestCaseProvider implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(final ExtensionContext context) throws Exception {
      final JsonNode testCases = JsonHelper.deserialize(MoreResources.readResource("json_avro_converter.json"));
      return MoreIterators.toList(testCases.elements()).stream().map(testCase -> Arguments.of(
          testCase.get("schemaName").asText(),
          testCase.get("namespace").asText(),
          testCase.get("jsonSchema"),
          testCase.get("jsonObject"),
          testCase.get("avroSchema"),
          testCase.get("avroObject"),
          testCase.has("name_transformer") && testCase.get("name_transformer").asBoolean()));
    }
  }

  @ParameterizedTest
  @ArgumentsSource(JsonToAvroConverterTestCaseProvider.class)
  public void testJsonToAvroConverter(final String schemaName,
                                      final String namespace,
                                      final JsonNode jsonSchema,
                                      final JsonNode jsonObject,
                                      final JsonNode avroSchema,
                                      final JsonNode avroObject,
                                      final boolean useNameTransformer)
      throws JsonProcessingException {
    final JsonAvroConverter objectConverter = JsonAvroConverter.builder()
        .setNameTransformer(useNameTransformer ? String::toUpperCase : Function.identity())
        .build();
    final Schema actualAvroSchema =  SCHEMA_CONVERTER.getAvroSchema(jsonSchema, schemaName, namespace, true);
    final GenericData.Record actualAvroObject = objectConverter.convertToGenericDataRecord(WRITER.writeValueAsBytes(jsonObject), actualAvroSchema);
    assertEquals(avroObject, JsonHelper.deserialize(actualAvroObject.toString()), String.format("Test for %s failed", schemaName));
  }

}

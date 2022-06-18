package influxbd;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvException;

public class InfluxDB2Example {

	public static void main(String[] args) throws IOException, CsvException {
		
		String token = "<your token>";
		String bucket = "<your bucket>";
		String org = "<your email>";

		InfluxDBClient client = InfluxDBClientFactory.create("https://us-east-1-1.aws.cloud2.influxdata.com",token.toCharArray());

		Reader reader = Files.newBufferedReader(Paths.get("your csv file"));
		CSVReader csvReader = new CSVReaderBuilder(reader).build();
		Point point = null;
		

		List<String[]> linhas = csvReader.readAll();
		for (String[] dado : linhas) {
			
			Instant dateInstant = getDate(dado[0]);
			
			point = Point.measurement("pep")
			        .addTag("Type", "Covid")
			        .addField("Positive", Integer.parseInt(dado[3]))
					.addField("Active", Integer.parseInt(dado[4]))
					.addField("Hospitalized", Integer.parseInt(dado[5]))
					.addField("Death", Integer.parseInt(dado[8]))
					.time(dateInstant, WritePrecision.NS);

		WriteApiBlocking writeApi = client.getWriteApiBlocking();
		writeApi.writePoint(bucket, org, point);
		}
		client.close();

	}
	
	private static Instant getDate(final String date) {
        final String[] dateHour = date.split(" ");
        return LocalDateTime.parse(
                        Stream.of(dateHour[0].split("/"))
                                .map(item -> StringUtils.leftPad(item, 2, "0"))
                                .collect(Collectors.joining("/"))
                                .concat(
                                        Stream.of(dateHour[1].split(":"))
                                                .map(item -> StringUtils.leftPad(item, 2, "0"))
                                                .collect(Collectors.joining(":"))
                                )
                        ,
                        DateTimeFormatter.ofPattern("dd/MM/yyyyHH:mm:ss")
                )
                .toInstant(ZoneOffset.UTC);
    }

}

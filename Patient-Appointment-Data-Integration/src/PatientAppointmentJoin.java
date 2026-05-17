import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * PatientAppointmentJoin
 *
 * A Hadoop MapReduce Reduce-Side Join that integrates two healthcare datasets:
 *   1. patients.txt   — 500 patient registration records
 *   2. appointments.csv — 500+ appointment records
 *
 * Join Key: Patient ID (LongWritable)
 * Output: Unified patient profile with appointment history.
 *
 * Real-World Use Case:
 *   Hospital operations teams use unified patient records for:
 *   - Pre-consultation doctor briefings
 *   - Appointment pattern analysis per department
 *   - Billing and insurance claim generation
 *   - Identifying inactive patients for follow-up campaigns
 */
public class PatientAppointmentJoin {

    // TAG constants for identifying the data source in the reducer
    private static final String PATIENT_TAG     = "PD";
    private static final String APPOINTMENT_TAG = "AD";
    private static final String DATA_SEPARATOR  = ",";
    private static final String TAG_SEPARATOR   = "~";

    // ─────────────────────────────────────────────────────────────
    //  PATIENT MAPPER
    //  Input:  PatientID,Name,DOB,ContactNumber,BloodType
    //  Output: (PatientID, "PD~Name,DOB,ContactNumber,BloodType")
    // ─────────────────────────────────────────────────────────────
    public static class PatientMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] fields = value.toString().split(DATA_SEPARATOR);
            if (fields.length < 2) return;

            try {
                long patientId = Long.parseLong(fields[0].trim());

                // Build tagged payload: "PD~<rest of fields>"
                StringBuilder payload = new StringBuilder(PATIENT_TAG + TAG_SEPARATOR);
                for (int i = 1; i < fields.length; i++) {
                    if (i > 1) payload.append(DATA_SEPARATOR);
                    payload.append(fields[i].trim());
                }

                context.write(new LongWritable(patientId), new Text(payload.toString()));

            } catch (NumberFormatException e) {
                // Skip header or malformed records
            }
        }
    }

    // ─────────────────────────────────────────────────────────────
    //  APPOINTMENT MAPPER
    //  Input:  PatientID,AppointmentDate,DoctorName,Department,Status
    //  Output: (PatientID, "AD~AppointmentDate,DoctorName,Department,Status")
    // ─────────────────────────────────────────────────────────────
    public static class AppointmentMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] fields = value.toString().split(DATA_SEPARATOR);
            if (fields.length < 2) return;

            try {
                long patientId = Long.parseLong(fields[0].trim());

                // Build tagged payload: "AD~<rest of fields>"
                StringBuilder payload = new StringBuilder(APPOINTMENT_TAG + TAG_SEPARATOR);
                for (int i = 1; i < fields.length; i++) {
                    if (i > 1) payload.append(DATA_SEPARATOR);
                    payload.append(fields[i].trim());
                }

                context.write(new LongWritable(patientId), new Text(payload.toString()));

            } catch (NumberFormatException e) {
                // Skip header or malformed records
            }
        }
    }

    // ─────────────────────────────────────────────────────────────
    //  REDUCER — Reduce-Side Join
    //  Receives all values for a given PatientID from both mappers.
    //  Identifies each record by its "PD~" or "AD~" tag prefix,
    //  then assembles the unified patient-appointment record.
    // ─────────────────────────────────────────────────────────────
    public static class PatientAppointmentReducer
            extends Reducer<LongWritable, Text, LongWritable, Text> {

        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String patientDetails     = null;
            String appointmentDetails = null;

            for (Text textValue : values) {
                String record = textValue.toString();
                String[] parts = record.split(TAG_SEPARATOR, 2);

                if (parts.length < 2) continue;

                String tag  = parts[0];
                String data = parts[1];

                if (tag.equalsIgnoreCase(PATIENT_TAG)) {
                    patientDetails = data;
                } else if (tag.equalsIgnoreCase(APPOINTMENT_TAG)) {
                    appointmentDetails = data;
                }
            }

            // Join: output whatever data is available
            String joinedRecord;
            if (patientDetails != null && appointmentDetails != null) {
                joinedRecord = patientDetails + DATA_SEPARATOR + appointmentDetails;
            } else if (patientDetails != null) {
                joinedRecord = patientDetails;  // Patient with no appointment
            } else if (appointmentDetails != null) {
                joinedRecord = appointmentDetails;  // Orphaned appointment record
            } else {
                return;  // Nothing to write
            }

            context.write(key, new Text(joinedRecord));
        }
    }

    // ─────────────────────────────────────────────────────────────
    //  DRIVER
    // ─────────────────────────────────────────────────────────────
    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println("Usage: PatientAppointmentJoin <patients-path> <appointments-path> <output-path>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = new Job(conf, "Patient-Appointment Reduce-Side Join");

        job.setJarByClass(PatientAppointmentJoin.class);
        job.setReducerClass(PatientAppointmentReducer.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        // MultipleInputs allows different mappers per input path
        MultipleInputs.addInputPath(job, new Path(args[0]),
                TextInputFormat.class, PatientMapper.class);

        MultipleInputs.addInputPath(job, new Path(args[1]),
                TextInputFormat.class, AppointmentMapper.class);

        Path outputPath = new Path(args[2]);
        FileOutputFormat.setOutputPath(job, outputPath);

        // Remove existing output directory
        outputPath.getFileSystem(conf).delete(outputPath, true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

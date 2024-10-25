use arrow::array::ArrayRef;
use arrow::array::Int32Array;
use arrow::array::StringArray;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::sync::Arc;
//use tempfile::tempfile;
use std::fs::File;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a schema
    let ids = Int32Array::from(vec![1, 2, 3, 4]);
    let vals = StringArray::from(vec![Some("a"), Some("b"), None, Some("d")]);
    let batch = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(ids) as ArrayRef),
        ("val", Arc::new(vals) as ArrayRef),
    ])
    .unwrap();

    let file = File::create("data.parquet")?;

    // WriterProperties can be used to set Parquet file options
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();

    writer.write(&batch).expect("Writing batch");

    // writer must be closed to write footer
    writer.close().unwrap();

    let file = File::open("data.parquet").unwrap();

    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    println!("Converted arrow schema is: {}", builder.schema());

    let mut reader = builder.build().unwrap();

    let record_batch = reader.next().unwrap().unwrap();

    println!("Read {} records.", record_batch.num_rows());
    println!("id: {:?}", record_batch.column(0));
    println!("val: {:?}", record_batch.column(1));

    Ok(())
}

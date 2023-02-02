
using Amazon.S3;
using Amazon.S3.Model;

var s3Client = new AmazonS3Client();

await using var inputStream = new FileStream("./movies.csv", FileMode.Open, FileAccess.Read);

var putObjectRequest = new PutObjectRequest
{
    BucketName = "nicksawsfirstbucket",
    Key = "movies/movies.csv",
    ContentType = "text/csv",
    InputStream = inputStream
};

await s3Client.PutObjectAsync(putObjectRequest);



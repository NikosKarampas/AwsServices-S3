using Amazon.S3;
using Amazon.S3.Model;
using Customers.Api.ImageSettings;
using Microsoft.Extensions.Options;

namespace Customers.Api.Services
{
    public class CustomerImageService : ICustomerImageService
    {
        private readonly IAmazonS3 _s3Client;
        private readonly IOptions<BucketSettings> _bucketSettings;

        public CustomerImageService(IAmazonS3 s3Client, IOptions<BucketSettings> bucketSettings)
        {
            _s3Client = s3Client;
            _bucketSettings = bucketSettings;
        }

        public async Task<PutObjectResponse> UploadImageAsync(Guid id, IFormFile file)
        {
            var putObjectRequest = new PutObjectRequest
            {
                BucketName = _bucketSettings.Value.Name,
                Key = $"images/{id}",
                ContentType = file.ContentType,                
                InputStream = file.OpenReadStream(),
                Metadata =
                {
                    ["x-amz-meta-originalname"] = file.FileName,
                    ["x-amz-meta-extension"] = Path.GetExtension(file.FileName)
                }
            };

            return await _s3Client.PutObjectAsync(putObjectRequest);
        }

        public async Task<GetObjectResponse> GetImageAsync(Guid id)
        {
            var getObjectRequest = new GetObjectRequest
            {
                BucketName = _bucketSettings.Value.Name,
                Key = $"images/{id}"
            };

            return await _s3Client.GetObjectAsync(getObjectRequest);
        }

        public async Task<DeleteObjectResponse> DeleteImageAsync(Guid id)
        {
            var deleteObjectRequest = new DeleteObjectRequest
            {
                BucketName = _bucketSettings.Value.Name,
                Key = $"images/{id}"
            };

            return await _s3Client.DeleteObjectAsync(deleteObjectRequest);
        }
    }
}

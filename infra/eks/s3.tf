# Payload bucket — spec.json, code packages, and the pickled step
# inputs/outputs that travel between the driver and the runner pods.
#
# No lifecycle expiry configured. Objects accumulate until someone decides
# on a retention policy; revisit before this grows into real money, since a
# single demand-forecast run writes on the order of 10 GB.

resource "aws_s3_bucket" "payload" {
  bucket = local.name

  tags = local.tags
}

resource "aws_s3_bucket_public_access_block" "payload" {
  bucket                  = aws_s3_bucket.payload.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "payload" {
  bucket = aws_s3_bucket.payload.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_versioning" "payload" {
  bucket = aws_s3_bucket.payload.id

  versioning_configuration {
    status = "Disabled"
  }
}

# Multipart uploads that fail mid-flight leave orphaned parts that are
# invisible in the console but still billed. Our uploads are routinely
# multi-GB, so this cleanup stays even with object expiry switched off.
resource "aws_s3_bucket_lifecycle_configuration" "abort_incomplete_uploads" {
  bucket = aws_s3_bucket.payload.id

  rule {
    id     = "abort-incomplete-multipart-uploads"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

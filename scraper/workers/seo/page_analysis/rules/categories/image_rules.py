"""
Images SEO Rules
Rules for image optimization, alt text, and performance.
"""

from ..base_seo_rule import BaseSEORuleV2


class BrokenImagesRule(BaseSEORuleV2):
    rule_id = "broken_images"
    rule_no = 79
    category = "Images"
    severity = "medium"
    description = "Broken images degrade UX, waste page requests, and leave alt text orphaned"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        images = normalized.get("images", [])
        
        for image in images:
            status = image.get("status")
            if status and status >= 400:
                issues.append(self.create_issue(
                    job_id, project_id, url,
                    f"Broken image found: {image.get('src')} (status: {status})",
                    f"Image {image.get('src')} returns {status}",
                    "200 OK status for all images",
                    data_key="images",
                    data_path=f"images.{image.get('src')}"
                ))
        
        return issues


class ImageFileSizeRule(BaseSEORuleV2):
    rule_id = "image_file_size"
    rule_no = 80
    category = "Images"
    severity = "medium"
    description = "Oversized images are the biggest contributor to slow page load times"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        images = normalized.get("images", [])
        
        for image in images:
            size = image.get("size")
            if size and size > 100 * 1024:  # 100KB in bytes
                size_kb = size / 1024
                issues.append(self.create_issue(
                    job_id, project_id, url,
                    f"Large image file: {image.get('src')} ({size_kb:.1f}KB)",
                    f"Image size: {size_kb:.1f}KB",
                    "Image < 100KB (hero images < 200KB)",
                    data_key="images",
                    data_path=f"images.{image.get('src')}.size"
                ))
        
        return issues


class ImagesMissingAltTextRule(BaseSEORuleV2):
    rule_id = "images_missing_alt_text"
    rule_no = 81
    category = "Images"
    severity = "high"
    description = "Missing alt text loses accessibility compliance, image search ranking, and AEO signals"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        images = normalized.get("images", [])
        
        for image in images:
            alt = image.get("alt")
            if alt is None or alt == "":
                issues.append(self.create_issue(
                    job_id, project_id, url,
                    f"Image missing alt text: {image.get('src')}",
                    f"No alt attribute for {image.get('src')}",
                    "Descriptive alt text for all content images",
                    data_key="images",
                    data_path=f"images.{image.get('src')}.alt"
                ))
        
        return issues


class ImagesNotWebPFormatRule(BaseSEORuleV2):
    rule_id = "images_not_webp_format"
    rule_no = 82
    category = "Images"
    severity = "medium"
    description = "JPEG/PNG files are 25–50% larger than WebP — switching is a free performance win"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        images = normalized.get("images", [])
        
        for image in images:
            src = image.get("src", "")
            mime_type = image.get("type", "")
            
            # Check file extension or MIME type
            is_jpeg_png = any(src.lower().endswith(ext) for ext in ['.jpg', '.jpeg', '.png'])
            is_webp_avif = src.lower().endswith(('.webp', '.avif')) or mime_type.lower() in ['image/webp', 'image/avif']
            
            if is_jpeg_png and not is_webp_avif:
                issues.append(self.create_issue(
                    job_id, project_id, url,
                    f"Image not in modern format: {image.get('src')}",
                    f"Image format: {src.split('.')[-1] if '.' in src else 'unknown'}",
                    "WebP or AVIF format",
                    data_key="images",
                    data_path=f"images.{image.get('src')}.format"
                ))
        
        return issues


class ImagesMissingDimensionsRule(BaseSEORuleV2):
    rule_id = "images_missing_dimensions"
    rule_no = 83
    category = "Images"
    severity = "medium"
    description = "Missing dimensions cause layout shift (CLS) as page loads — direct CWV impact"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        images = normalized.get("images", [])
        
        for image in images:
            width = image.get("width")
            height = image.get("height")
            
            if not width or not height:
                issues.append(self.create_issue(
                    job_id, project_id, url,
                    f"Image missing dimensions: {image.get('src')}",
                    f"Width: {width}, Height: {height}",
                    "Explicit width and height attributes",
                    data_key="images",
                    data_path=f"images.{image.get('src')}.dimensions"
                ))
        
        return issues


class ImagesWithoutLazyLoadingRule(BaseSEORuleV2):
    rule_id = "images_without_lazy_loading"
    rule_no = 84
    category = "Images"
    severity = "medium"
    description = "Loading all images on page load wastes bandwidth and delays above-fold rendering"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        images = normalized.get("images", [])
        
        for i, image in enumerate(images):
            loading = image.get("loading")
            
            # Skip first few images (likely above-fold)
            if i < 3:
                continue
                
            if loading != "lazy":
                issues.append(self.create_issue(
                    job_id, project_id, url,
                    f"Below-fold image not lazy loaded: {image.get('src')}",
                    f"Loading attribute: {loading}",
                    "loading='lazy' for non-critical images",
                    data_key="images",
                    data_path=f"images.{image.get('src')}.loading"
                ))
        
        return issues


def register_image_rules(registry):
    """Register all image rules with the registry."""
    registry.register(BrokenImagesRule())
    registry.register(ImageFileSizeRule())
    registry.register(ImagesMissingAltTextRule())
    registry.register(ImagesNotWebPFormatRule())
    registry.register(ImagesMissingDimensionsRule())
    registry.register(ImagesWithoutLazyLoadingRule())

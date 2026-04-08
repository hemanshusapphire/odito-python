"""Video Enhancer - Enhanced video extraction and analysis.

This module is deprecated. Video functionality has been moved to multimedia_extractor.py.
This file exists for backward compatibility only.
"""

# Import from multimedia_extractor for backward compatibility
try:
    from multimedia_extractor import MultimediaExtractor
except ImportError:
    from .multimedia_extractor import MultimediaExtractor

class VideoEnhancer:
    """Deprecated: Use MultimediaExtractor instead."""
    
    def __init__(self, soup, entities, base_url):
        # Forward to multimedia extractor
        self.multimedia_extractor = MultimediaExtractor(soup, entities, base_url)
    
    def extract_enhanced_videos(self):
        """Extract enhanced video data (deprecated - use multimedia_extractor)."""
        multimedia_data = self.multimedia_extractor.extract_all_multimedia()
        return multimedia_data.get('enhanced_multimedia', {})

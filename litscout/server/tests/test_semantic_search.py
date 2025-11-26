# server/tests/test_semantic_search.py

"""
Tests for the semantic search functionality.
"""

import pytest
from unittest.mock import patch, MagicMock
import math

from server.search.semantic import (
    _cosine_similarity,
    SearchResult,
    format_search_results,
)


class TestCosineSimilarity:
    """Tests for the cosine similarity function."""

    def test_identical_vectors(self):
        """Identical vectors should have similarity of 1.0."""
        vec = [1.0, 0.0, 0.0]
        assert _cosine_similarity(vec, vec) == pytest.approx(1.0)

    def test_orthogonal_vectors(self):
        """Orthogonal vectors should have similarity of 0.0."""
        vec1 = [1.0, 0.0]
        vec2 = [0.0, 1.0]
        assert _cosine_similarity(vec1, vec2) == pytest.approx(0.0)

    def test_opposite_vectors(self):
        """Opposite vectors should have similarity of -1.0."""
        vec1 = [1.0, 0.0]
        vec2 = [-1.0, 0.0]
        assert _cosine_similarity(vec1, vec2) == pytest.approx(-1.0)

    def test_normalized_vectors(self):
        """Test with pre-normalized vectors."""
        # 45 degree angle vectors (normalized)
        vec1 = [1.0, 0.0]
        vec2 = [math.sqrt(2)/2, math.sqrt(2)/2]
        # Cosine of 45 degrees is sqrt(2)/2 ≈ 0.707
        assert _cosine_similarity(vec1, vec2) == pytest.approx(math.sqrt(2)/2, rel=0.01)

    def test_different_length_vectors(self):
        """Vectors of different lengths should return 0.0."""
        vec1 = [1.0, 0.0, 0.0]
        vec2 = [1.0, 0.0]
        assert _cosine_similarity(vec1, vec2) == 0.0

    def test_zero_vector(self):
        """Zero vectors should return 0.0."""
        vec1 = [0.0, 0.0, 0.0]
        vec2 = [1.0, 0.0, 0.0]
        assert _cosine_similarity(vec1, vec2) == 0.0


class TestSearchResult:
    """Tests for the SearchResult dataclass."""

    def test_search_result_creation(self):
        """Test creating a SearchResult."""
        result = SearchResult(
            paper_id=1,
            title="Test Paper",
            abstract="This is a test abstract",
            year=2023,
            doi="10.1234/test",
            score=0.95,
        )
        assert result.paper_id == 1
        assert result.title == "Test Paper"
        assert result.abstract == "This is a test abstract"
        assert result.year == 2023
        assert result.doi == "10.1234/test"
        assert result.score == 0.95

    def test_search_result_optional_fields(self):
        """Test SearchResult with optional fields as None."""
        result = SearchResult(
            paper_id=1,
            title="Test Paper",
            abstract=None,
            year=None,
            doi=None,
            score=0.5,
        )
        assert result.abstract is None
        assert result.year is None
        assert result.doi is None


class TestFormatSearchResults:
    """Tests for the format_search_results function."""

    def test_empty_results(self):
        """Empty results should return appropriate message."""
        output = format_search_results([])
        assert output == "No results found."

    def test_format_single_result(self):
        """Test formatting a single result."""
        results = [
            SearchResult(
                paper_id=1,
                title="Test Paper",
                abstract="Test abstract",
                year=2023,
                doi="10.1234/test",
                score=0.95,
            )
        ]
        output = format_search_results(results)
        assert "Found 1 result" in output
        assert "[0.950]" in output
        assert "Test Paper" in output
        assert "Year: 2023" in output
        assert "DOI: 10.1234/test" in output

    def test_format_multiple_results(self):
        """Test formatting multiple results."""
        results = [
            SearchResult(
                paper_id=1, title="Paper 1", abstract=None,
                year=2023, doi=None, score=0.9,
            ),
            SearchResult(
                paper_id=2, title="Paper 2", abstract=None,
                year=2022, doi=None, score=0.8,
            ),
        ]
        output = format_search_results(results)
        assert "Found 2 results" in output
        assert "Paper 1" in output
        assert "Paper 2" in output

    def test_format_verbose_with_abstract(self):
        """Test verbose formatting includes abstract."""
        results = [
            SearchResult(
                paper_id=1,
                title="Test Paper",
                abstract="This is a detailed abstract about the paper.",
                year=2023,
                doi=None,
                score=0.95,
            )
        ]
        output = format_search_results(results, verbose=True)
        assert "Abstract:" in output
        assert "detailed abstract" in output

    def test_format_verbose_truncates_long_abstract(self):
        """Test that long abstracts are truncated in verbose mode."""
        long_abstract = "A" * 500
        results = [
            SearchResult(
                paper_id=1,
                title="Test Paper",
                abstract=long_abstract,
                year=2023,
                doi=None,
                score=0.95,
            )
        ]
        output = format_search_results(results, verbose=True)
        assert "Abstract:" in output
        assert "..." in output
        # Should be truncated to 300 chars + "..."
        assert "A" * 300 in output


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

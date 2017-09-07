def xcheck_author(record, result):
    """
    Are authors consistent?
    Compare first 5 authors of both records
    """
    from inspire_json_merger.comparators import AuthorComparator
    score = 0.5
    if not record.get('authors'):
        return score
    if not result.record.get('authors'):
        return score
    try:
        authors_record = min(len(record['authors']), 5)
        authors_match_record = min(len(result.record['authors']), 5)
        matches = len(
            AuthorComparator(
                record['authors'][:authors_record],
                result.record['authors'][:authors_match_record]
            ).matches
        )
        score = matches / float(max(authors_record, authors_match_record))
    except:
        # FIXME json_merger fails internally in some author comparison
        pass
    return score

def xcheck_author_var(record, result):
    """
    Are authors consistent?
    Is the number of authors consistent? Slightly different -> degrade score
    Are the first 2 authors in the matched record?
    No authors in either record -> ignore
    """
    import math
    from inspire_json_merger.comparators import AuthorComparator
    score = None
    if not record.get('authors'):
        return score
    if not result.record.get('authors'):
        return score

    num_authors_record = len(record['authors'])
    num_authors_match = len(result.record['authors'])
    num_score = 1.0
    if (num_authors_record > 1 and num_authors_match > 1):
        if (abs(num_authors_record - num_authors_match) > 3 and
            abs(math.log(num_authors_record / float(num_authors_match))) > 0.3):
            return 0
        if (abs(num_authors_record - num_authors_match) > 2 and
            abs(math.log(num_authors_record / float(num_authors_match))) > 0.2):
            num_score = 0.7

    try:
        num_authors_record_2 = min(num_authors_record, 2)
        matches = len(
            AuthorComparator(
                record['authors'][:num_authors_record_2],
                result.record['authors']
            ).matches
        )
        score = matches / float(max(num_authors_record_2, num_authors_match))
        score = score * num_score
    except:
        # FIXME json_merger fails internally in some author comparison
        pass
    return score


def xcheck_title(record, result):
    """
    Is title consistent?
    Count words common in both titles.
    No title in either record -> ignore
    """
    from itertools import product
    if not record.get('titles'):
        return None
    if not result.record.get('titles'):
        return None

    frequent_words = set(['a', 'at', 'to', 'on', 'of', 'in',
                   'and', 'for', 'not', 'the', 'with', 'from'])
    record_titles = [r['title'].lower() for r in record['titles']]
    result_titles = [r['title'].lower() for r in result.record['titles']]

    max_score = 0
    for titles in product(record_titles, result_titles):
        record_tokens = set(titles[0].split()) - frequent_words
        result_tokens = set(titles[1].split()) - frequent_words
        try:
            score = len(record_tokens & result_tokens) / \
                float(len(record_tokens | result_tokens))
        except:
            score = 0
        if score > max_score:
            max_score = score
    return max_score

# need to install Github python package

from github import Github
from datetime import datetime
import os

feat_list = []
fix_list = []
test_list = []
perf_list = []
docs_list = []
chore_list = []
cats_name_dict = {'feat': 'New Feature', 'fix': 'Bug Fix', 'test': 'Test',
                  'perf': 'Performance', 'chore': 'Enhancement', 'docs': 'Documentation'}
cats_dict = {'feat': feat_list, 'fix': fix_list,
             'perf': perf_list, 'chore': chore_list, 'test': test_list, 'docs': docs_list}

# Please make sure setup system environment GITHUB_TOKEN with 'github_pat_xxx'
access_token = os.getenv('GITHUB_TOKEN')
# access the GitHub API with an access token
g = Github(access_token)

repo = g.get_repo("ByConity/ByConity")

# setup the datatime of last release
last_relase_date = datetime(2023, 5, 24)

pulls = repo.get_pulls(state='close', sort='created', base='master')
for pr in pulls:
    # Filter the PRs which is new for this release
    if pr.updated_at > last_relase_date:
        # Checkout the PR title with prefix
        index = pr.title.find(': ')
        if index > -1:
            title = pr.title[index + 2:]
            cat = pr.title[:index]
            cat_list = cats_dict.get(cat)
            if cat_list is not None:
                cat_list.append('[' + title + ']' + '(' + pr.html_url + ')')

# Print all the items in a markdown format
for k in cats_dict.keys():
    print('## ' + cats_name_dict.get(k))
    cat_list = cats_dict.get(k)
    for item in cat_list:
        print(" * " + item)
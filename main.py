import argparse
import requests
import re
import simplejson as json
import multiprocessing

solar_url = 'http://example.com/solr/collection1/select?q=host%3A{{url}}&rows=0&wt=json&indent=true&facet=true&facet.field=url_depth'

def replace_str(replaced_str, new_str, input_url):
    return re.sub(replaced_str, new_str, input_url)

def get_opt():
    parser = argparse.ArgumentParser(description='%(prog)s')
    parser.add_argument("--file", '-f', type=str, required=True, dest='file_name',
                        help="Path to data file", metavar='file_name')
    parser.add_argument("--out", '-o', type=str, required=True, dest='output_file_name',
                        help="Path to output file", metavar='output_file_name')
    parser.add_argument("--thread", '-t', type=int, dest='threads_count',
                        help="Threads count", metavar='threads count')

    args = vars(parser.parse_args())

    return args

def get_data(url):
    data = json.loads(requests.get(url).content)
    try:
        print data["responseHeader"]["params"]["q"]
        url_depth_1 = str(data["facet_counts"]["facet_fields"]["url_depth"][1])
        url_depth_2 = str(data["facet_counts"]["facet_fields"]["url_depth"][3])
        url_depth_3 = str(data["facet_counts"]["facet_fields"]["url_depth"][5])
        return url_depth_1 + ';' + url_depth_2 + ';' + url_depth_3 + '\n'
    except Exception as inst:
        return "0;0;0\n"

def read_file(file_name):
    with open(file_name) as f:
        urls_list = f.readlines()

    return urls_list

def run(output_file_name, urls_list):
    with open(output_file_name, 'w') as f:
        # f.write('url;url_depth_1;url_depth_2;url_depth_3\n')
        f = open(output_file_name,'w')
        for url in urls_list:
            clean_url = replace_str('http:|/', "", url)
            line = clean_url.rstrip() + ";" + get_data(replace_str('\{\{url\}\}', clean_url, solar_url))
            f.write(line)
        f.close()

if __name__ == '__main__':
    log = object
    try:
        args = get_opt()
        urls_list = read_file(args["file_name"])

        # list1a=urls_list[:10]
        # list1b=urls_list[10:]
        #
        jobs = []
        for i in range(args["threads_count"]):
            p1 = multiprocessing.Process(target=run, args=(args["output_file_name"], urls_list))
            jobs.append(p1)
            p1.start()

       # run(args["output_file_name"], urls_list)
    except Exception as inst:
        print (inst)

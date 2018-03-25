import io
import concurrent.futures
import time
import os

import spacy
# need to run python -m spacy download en

nlp = spacy.load('en', parser=False)

INITIAL_SENTENCES = [
	nlp(u'How now brown cow.'),
	nlp(u'Tread softly because you tread on my dreams.'),
	nlp(u'I did not inhale.'),
	nlp(u'But the cops, they pulled me over.'),
	nlp(u'No Mr. Bond, I expect you to die.')
]


def get_tokens(phrase):
	tokens = [token.pos_ for token in phrase]
	return tokens


def transform(phrase):
	print(f'Process {os.getpid()} working on phrase beginning: {phrase[:3]}')
	tokens = get_tokens(phrase)
	print(f'Process {os.getpid()} completed work on beginning: {phrase[:3]}')
	return {phrase: tokens}


def io_intensive_transform(count_list):	
	s = io.StringIO()
	for count in count_list:
		s.write('Hello World{count}\n')


def cpu_intensive_transform(n):
	if n == 0:
		return 0
	elif n == 1:
		return 1
	else:
		return cpu_intensive_transform(n-1) + cpu_intensive_transform(n-2)


def run_pool(*, strategy, max_workers=4, sample_text=INITIAL_SENTENCES):
	start = time.time()
	with strategy(max_workers=max_workers) as executor:
		# comment accordingly to time IO vs. CPU intensive tasks
		result = executor.map(transform, sample_text)
		result = executor.map(io_intensive_transform, range(1,100000))
		result = executor.map(cpu_intensive_transform, [30, 31, 32, 33, 34, 35])

	end = time.time()

	print(f'\nTime to complete using {strategy}, with {max_workers} workers: {end - start:.2f}s\n')
	print(result) # returns an iterator


if __name__=='__main__':
	max_workers = 4
	sample_text = INITIAL_SENTENCES

	# Execution strategy 1: Using processes
	run_pool(strategy=concurrent.futures.ProcessPoolExecutor,
			 max_workers=max_workers,
			 sample_text=sample_text)
	# Execution strategy 2: Using threads
	run_pool(strategy=concurrent.futures.ThreadPoolExecutor,
			 max_workers=max_workers,
			 sample_text=sample_text)
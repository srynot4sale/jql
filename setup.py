from setuptools import setup

setup(name='jql',
      version='0.1',
      description='Opinionated database',
      url='http://github.com/srynot4sale/jql',
      author='Aaron Barnes',
      author_email='aaron@io.nz',
      license='MIT',
      packages=['jql', 'jql.store'],
      package_data={'jql': ['jql.lark']},
      install_requires=[
          'lark',
          'structlog',
          'colorama',
          'hashids',
          'prompt_toolkit',
          'pynamodb',
          'huey',
          'gevent',
          'lupa'
      ],
      zip_safe=False)

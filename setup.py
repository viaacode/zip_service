from setuptools import setup

setup(name='viaa-zipper',
      version='0.1',
      description='VIAA ZIP creator.',
      long_description='Zip File Creator based on RabbitMQ messages.',
      classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: © 2015 VIAA',
        'Programming Language :: Python :: 3.4',
        'Topic :: File Creation',
      ],
      keywords='zip rabbit worker',
      author='Jonas Liekens',
      author_email='jonas.liekens@c4j.be',
      license='© 2015 VIAA',
      packages=['zipper'],
      install_requires=[
          'pika',
      ],
      entry_points={
          'console_scripts': ['zipper=zipper.command_line:main'],
      },
      include_package_data=True,
      zip_safe=False)

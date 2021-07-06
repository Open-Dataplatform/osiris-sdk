import setuptools

setuptools.setup(
    install_requires=['msal', 'requests', 'azure-identity', 'apache_beam', 'azure-storage-file-datalake',
                      'pandas', 'prometheus_client', 'aiohttp', 'pyarrow']
)


import setuptools

setuptools.setup(
    install_requires=['msal', 'requests', 'azure-identity', 'apache_beam==2.28.0', 'azure-storage-file-datalake',
                      'pandas']
)


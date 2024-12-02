# ==== Purpose ====
#
# Ensure that the utility 'curl' is found and is at least version 7.76.0.
# Otherwise, fail the test.

--perl
use strict;
use warnings;

# Minimum required version
my @min_version = (7, 76, 0);

# Execute curl --version and capture the output
my $curl_version_output = `curl --version 2>&1`;
if ($?) {
    die "curl utility not found or not executable.\n";
}

# Extract the version number from the output
if ($curl_version_output =~ /curl\s+(\d+)\.(\d+)\.(\d+)/) {
    my @version = ($1, $2, $3);
    # Compare version components
    for my $i (0..$#min_version) {
        if ($version[$i] > $min_version[$i]) {
            last; # Current version is newer
        }
        if ($version[$i] < $min_version[$i]) {
            die "Found curl version $version[0].$version[1].$version[2], which".
                " is too old. Version $min_version[0].$min_version[1].".
                "$min_version[2] or newer is required.\n";
        }
    }
} else {
    die "Unable to determine curl version from output: $curl_version_output\n";
}
EOF

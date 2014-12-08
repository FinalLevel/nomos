Summary: Nomos Storage is a key-value, persistent and high available server, which is simple but extremely fast
Name: nomos
Version: %{ver}
Release: %{rel}
License: BSD
URL: https://github.com/FinalLevel/nomos
Source0: %{name}-%{version}-%{release}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}
Requires(pre): /usr/sbin/useradd, /usr/bin/getent

%description
Nomos Storage is a key-value, persistent and high available server, which is simple but extremely fast

%prep
%setup -q -n %{name}-%{version}-%{release}

%build
%configure --prefix="%{?_program_prefix}" --sysconfdir=/etc --enable-debug
make

%install
rm -rf %{buildroot} 

make DESTDIR=%{buildroot} install

mkdir -p %{buildroot}/etc/logrotate.d
cp etc/nomos.logrotate %{buildroot}/etc/logrotate.d/nomos

mkdir -p %{buildroot}/etc/init.d
%{__install} -D -m 755 etc/init.d/nomos %{buildroot}/etc/init.d/nomos


%pre
/usr/bin/getent group nomos || /usr/sbin/groupadd -r nomos
/usr/bin/getent passwd nomos || /usr/sbin/useradd -r -s /sbin/nologin -g nomos nomos

%files
%defattr(-, root, root)
/usr/bin/nomos
/usr/bin/nomos_wrapper.sh
/etc/init.d/nomos

%config(noreplace) /etc/logrotate.d/nomos
%config(noreplace) /etc/nomos.cnf

%clean
rm -rf %{buildroot}

#The changelog is built automatically from Git history
%changelog

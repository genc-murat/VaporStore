document.addEventListener('DOMContentLoaded', () => {
    // Reveal animations on scroll
    const reveals = document.querySelectorAll('.reveal');

    const revealCallback = (entries, observer) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                entry.target.classList.add('active');
            }
        });
    };

    const revealObserver = new IntersectionObserver(revealCallback, {
        threshold: 0.1
    });

    reveals.forEach(reveal => {
        revealObserver.observe(reveal);
    });

    // Scroll progress for navbar
    const nav = document.querySelector('nav');
    window.addEventListener('scroll', () => {
        if (window.scrollY > 50) {
            nav.style.backgroundColor = 'rgba(2, 6, 23, 0.8)';
            nav.style.boxShadow = '0 10px 30px rgba(0, 0, 0, 0.5)';
        } else {
            nav.style.backgroundColor = 'transparent';
            nav.style.boxShadow = 'none';
        }
    });
});

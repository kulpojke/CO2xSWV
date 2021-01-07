#/* cSpell:disable */
#%%
 
import daft as daft
import matplotlib as mpl
import matplotlib.pyplot as plt


# %%
mpl.rc('font', size=18)

pgm = daft.PGM([13.6, 3.4], origin=[1.15, 1.0], node_ec='none')

pgm.add_node(daft.Node('model', r'model', 2.0, 4))
pgm.add_node(daft.Node('parameter', r'parameter', 2.0, 3))
pgm.add_node(daft.Node('observations', r'observations', 2.0, 2))

pgm.add_node(daft.Node('mu_sigma', r'$\mu,\sigma^2$', 5.5, 4))
pgm.add_node(daft.Node('theta_0', r'$\theta_0$', 4, 3))
pgm.add_node(daft.Node('theta_1', r'$\theta_1$', 5, 3))
pgm.add_node(daft.Node('theta_dots', r'$\cdots$', 6, 3))
pgm.add_node(daft.Node('theta_k', r'$\theta_k$', 7, 3))
pgm.add_node(daft.Node('y_0', r'$y_0$', 4, 2))
pgm.add_node(daft.Node('y_1', r'$y_1$', 5, 2))
pgm.add_node(daft.Node('y_dots', r'$\cdots$', 6, 2))
pgm.add_node(daft.Node('y_k', r'$y_k$', 7, 2))
pgm.add_edge('mu_sigma', 'theta_0')
pgm.add_edge('mu_sigma', 'theta_1')
pgm.add_edge('mu_sigma', 'theta_k')
pgm.add_edge('theta_0', 'y_0')
pgm.add_edge('theta_1', 'y_1')
pgm.add_edge('theta_k', 'y_k')
pgm.render()
plt.show()

#%%
# pooled

mpl.rc('font', size=12)
pgm = daft.PGM([7, 4.5], node_unit=1.2)
pgm.add_node(
    daft.Node(
        'mu_beta_1_prior',
        r'$\mathcal{N}(0, 10^5)$',
        1,
        4,
        fixed=True,
        offset=(0, 5)),
    daft.Node(
        'mu_beta_2_prior',
        r'$\mathcal{N}(0, 10^5)$',
        1,
        4,
        fixed=True,
        offset=(0, 5))    
        )
pgm.add_node(
    daft.Node(
        'sigma_beta_prior',
        r'$\mathrm{HalfCauchy}(0, 5)$',
        3,
        4,
        fixed=True,
        offset=(10, 5)))
pgm.add_node(
    daft.Node(
        'sigma_prior',
        r'$\mathrm{HalfCauchy}(0, 5)$',
        4,
        3,
        fixed=True,
        offset=(20, 5)))

pgm.add_node(daft.Node('mu_beta_0', r'$\mu_\beta_0$', 1, 3))
pgm.add_node(daft.Node('mu_beta_1', r'$\mu_\beta_1$', 1, 3))
pgm.add_node(daft.Node('mu_beta_2', r'$\mu_\beta_2$', 1, 3))


pgm.add_node(daft.Node('sigma_beta', r'$\sigma_\beta$', 3, 3))
pgm.add_node(daft.Node('beta', r'$\beta \sim \mathcal{N}$', 2, 2, scale=1.25))
pgm.add_node(daft.Node('sigma_y', r'$\sigma_y$', 4, 2))
pgm.add_node(
    daft.Node(
        'y_i', r'$y_i \sim \mathcal{N}$', 3.25, 1, scale=1.25, observed=True))

pgm.add_edge('mu_beta_1_prior', 'mu_beta_1')

pgm.add_edge('sigma_beta_prior', 'sigma_beta')

pgm.add_edge('mu_beta_1', 'beta')

pgm.add_edge('sigma_beta', 'beta')
pgm.add_edge('sigma_prior', 'sigma_y')
pgm.add_edge('sigma_y', 'y_i')
pgm.add_edge('beta', 'y_i')
pgm.add_plate(daft.Plate([1.4, 1.2, 1.2, 1.4], '$n$'))
pgm.add_plate(daft.Plate([2.65, 0.2, 1.2, 1.4], '$n$'))

pgm.render()
plt.show()

# %%
# partial pooling

mpl.rc('font', size=12)
pgm = daft.PGM([7, 4.5], node_unit=1.2)
pgm.add_node(
    daft.Node(
        'mu_beta_prior',
        r'$\mathcal{N}(0, 10^5)$',
        1,
        4,
        fixed=True,
        offset=(0, 5)))
pgm.add_node(
    daft.Node(
        'sigma_beta_prior',
        r'$\mathrm{HalfCauchy}(0, 5)$',
        3,
        4,
        fixed=True,
        offset=(10, 5)))
pgm.add_node(
    daft.Node(
        'sigma_prior',
        r'$\mathrm{HalfCauchy}(0, 5)$',
        4,
        3,
        fixed=True,
        offset=(20, 5)))
pgm.add_node(daft.Node('mu_beta', r'$\mu_\beta$', 1, 3))
pgm.add_node(daft.Node('sigma_beta', r'$\sigma_\beta$', 3, 3))
pgm.add_node(daft.Node('beta', r'$\beta \sim \mathcal{N}$', 2, 2, scale=1.25))
pgm.add_node(daft.Node('sigma_y', r'$\sigma_y$', 4, 2))
pgm.add_node(
    daft.Node(
        'y_i', r'$y_i \sim \mathcal{N}$', 3.25, 1, scale=1.25, observed=True))
pgm.add_edge('mu_beta_prior', 'mu_beta')
pgm.add_edge('sigma_beta_prior', 'sigma_beta')
pgm.add_edge('mu_beta', 'beta')
pgm.add_edge('sigma_beta', 'beta')
pgm.add_edge('sigma_prior', 'sigma_y')
pgm.add_edge('sigma_y', 'y_i')
pgm.add_edge('beta', 'y_i')
pgm.add_plate(daft.Plate([1.4, 1.2, 1.2, 1.4], '$n$'))
pgm.add_plate(daft.Plate([2.65, 0.2, 1.2, 1.4], '$n$'))

pgm.render()
plt.show()

# %%
# %%
# complete pooling

mpl.rc('font', size=12)

muB0x  = 1.5
sigB0x = 3
B0x = muB0x + (sigB0x - muB0x) / 2

muB1x = 4.5
sigB1x = 6
B1x = muB1x + (sigB1x - muB1x) / 2

muB2x = 7.5
sigB2x = 9
B2x = muB2x + (sigB2x - muB2x) / 2

pri_y = 6.5
pri_y_ = 7
hyper_y = 5.5
param_y = 4

pgm = daft.PGM([12, 9], node_unit=1.2)

pgm.add_node(daft.Node('mu_beta_0_prior', r'$\mathcal{N}(0, 10^5)$',
        muB0x, pri_y, fixed=True, offset=(0, 5)))
pgm.add_node(daft.Node('mu_beta_1_prior', r'$\mathcal{N}(0, 10^5)$',
        muB1x, pri_y, fixed=True, offset=(0, 5)))
pgm.add_node(daft.Node('mu_beta_2_prior', r'$\mathcal{N}(0, 10^5)$',
        muB2x, pri_y, fixed=True, offset=(0, 5)))

pgm.add_node(daft.Node('sigma_beta_prior_0', r'$\mathrm{HalfCauchy}(0, 5)$', 
            sigB0x, pri_y_, fixed=True, offset=(10, 5)))
pgm.add_node(daft.Node('sigma_beta_prior_1', r'$\mathrm{HalfCauchy}(0, 5)$', 
            sigB1x, pri_y_, fixed=True, offset=(10, 5)))
pgm.add_node(daft.Node('sigma_beta_prior_2', r'$\mathrm{HalfCauchy}(0, 5)$', 
            sigB2x, pri_y_, fixed=True, offset=(10, 5)))




        
pgm.add_node(daft.Node('mu_beta_0', r'$\mu_{\beta0}$', muB0x, hyper_y))
pgm.add_node(daft.Node('mu_beta_1', r'$\mu_{\beta1}$', muB1x, hyper_y))
pgm.add_node(daft.Node('mu_beta_2', r'$\mu_{\beta2}$', muB2x, hyper_y))

pgm.add_node(daft.Node('sigma_beta_0', r'$\sigma_{\beta0}$', sigB0x, hyper_y))
pgm.add_node(daft.Node('sigma_beta_1', r'$\sigma_{\beta1}$', sigB1x, hyper_y))
pgm.add_node(daft.Node('sigma_beta_2', r'$\sigma_{\beta2}$', sigB2x, hyper_y))

pgm.add_node(daft.Node('beta0', r'$\beta_0 \sim \mathcal{N}$', B0x, param_y, scale=1.25))
pgm.add_plate(daft.Plate([B0x-0.6, param_y-0.8, 1.2, 1.4], '$n$'))

pgm.add_node(daft.Node('beta1', r'$\beta_1 \sim \mathcal{N}$', B1x, param_y, scale=1.25))
pgm.add_plate(daft.Plate([B1x-0.6, param_y-0.8, 1.2, 1.4], '$n$'))

pgm.add_node(daft.Node('beta2', r'$\beta_2 \sim \mathcal{N}$', B2x, param_y, scale=1.25))
pgm.add_plate(daft.Plate([B2x-0.6, param_y-0.8, 1.2, 1.4], '$n$'))


pgm.add_node(daft.Node('sigma_prior',r'$\mathrm{HalfCauchy}(0, 5)$'
            ,sigB2x, 2.5, fixed=True, offset=(20, 5)))
pgm.add_node(daft.Node('sigma_y', r'$\sigma_y$', sigB2x, 2))

pgm.add_node(daft.Node('y_i', r'$y_i \sim \mathcal{N}$', 5.25, 0.75-0.5,
            scale=1.25, observed=True))

# edges from priors to hypers 
pgm.add_edge('mu_beta_0_prior', 'mu_beta_0')
pgm.add_edge('mu_beta_1_prior', 'mu_beta_1')
pgm.add_edge('mu_beta_2_prior', 'mu_beta_2')

pgm.add_edge('sigma_beta_prior_0', 'sigma_beta_0')
pgm.add_edge('sigma_beta_prior_1', 'sigma_beta_1')
pgm.add_edge('sigma_beta_prior_2', 'sigma_beta_2')

# edges hypers to params
pgm.add_edge('mu_beta_0', 'beta0')
pgm.add_edge('sigma_beta_0', 'beta0')

pgm.add_edge('mu_beta_1', 'beta1')
pgm.add_edge('sigma_beta_1', 'beta1')

pgm.add_edge('mu_beta_2', 'beta2')
pgm.add_edge('sigma_beta_2', 'beta2')

# edges params to posterior
pgm.add_edge('beta0', 'y_i')
pgm.add_edge('beta1', 'y_i')
pgm.add_edge('beta2', 'y_i')



pgm.add_edge('sigma_prior', 'sigma_y')
pgm.add_edge('sigma_y', 'y_i')

pgm.add_plate(daft.Plate([1.4+3.25, 0-0.5, 1.2, 1.4], '$n$'))

pgm.render()
plt.show()

# %%
dudes = 'vako, kbazzano, sdas01, jbdavies, soewing, mgambero, ngenshaf, mhood01, khunyada, jblucero, ibnorris, spanian, apatil03, dlrasmus, garichar, jrodr386, abtomlin, swatt, ejwinn, cuwoods'
# %%
dudes = dudes.split(',')
# %%
dudes = [dude.strip() + '@calpoly.edu' for dude in dudes]
# %%
dudes
# %%
